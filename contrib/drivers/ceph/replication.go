// Copyright 2018 The OpenSDS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package ceph

import (
	"github.com/astaxie/beego/logs"
	log "github.com/golang/glog"
	. "github.com/opensds/opensds/contrib/drivers/utils/config"
	"github.com/opensds/opensds/pkg/model"
	pb "github.com/opensds/opensds/pkg/model/proto"
	"github.com/opensds/opensds/pkg/utils/config"
	"github.com/opensds/opensds/pkg/utils/pwd"
	"golang.org/x/crypto/ssh"
	"time"
)

type ReplicationDriver struct {
	conf *Config
}

type Replication struct {
	IPaddresshost string `yaml:"hostip,omitempty"`
	Ipaddresspeer string `yaml:"peerip,omitempty"`
	Username      string `yaml:"username,omitempty"`
	Password      string `yaml:"password,omitempty"`
	HostDialIP    string `yaml:"hostDialIP,omitempty"`
	PeerDialIP    string `yaml:"peerDialIP,omitempty"`
	FilePath      string `yaml:"filePath,omitempty"`
}

type Config struct {
	ConfigFile  string `yaml:"configFile,omitempty"`
	Replication `yaml:"replication"`
}

// Authentication

func (r *ReplicationDriver) CephClient() *ssh.Client {

	pwdCiphertext := r.conf.Password
	// password Encrypt
	pwdTool := pwd.NewAES()
	pwdEncrypt, err := pwdTool.Encrypter(pwdCiphertext)
	if err != nil {
		log.Error("failed to encrypt ssh password ", err)
	}
	// password Decrypt
	pwdDecrypt, err := pwdTool.Decrypter(pwdEncrypt)
	if err != nil {
		log.Error("failed to decrypt ssh password ", err)
	}

	cephconfig := &ssh.ClientConfig{
		User: r.conf.Username,
		Auth: []ssh.AuthMethod{
			ssh.Password(pwdDecrypt),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	cephclient, err := ssh.Dial("tcp", r.conf.HostDialIP, cephconfig)
	if err != nil {
		log.Error("failed to dial: " + err.Error())

	}
	return cephclient
}

func (r *ReplicationDriver) BackupClient() *ssh.Client {
	backupconfig := &ssh.ClientConfig{
		User: r.conf.Username,
		Auth: []ssh.AuthMethod{
			ssh.Password(r.conf.Password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	backupclient, error := ssh.Dial("tcp", r.conf.PeerDialIP, backupconfig)

	if error != nil {
		log.Error("failed to dial: " + error.Error())

	}
	return backupclient

}

// Setup
func (r *ReplicationDriver) Setup() error {

	r.conf = &Config{}
	p := config.CONF.OsdsDock.Backends.Ceph.ConfigPath
	if "" == p {
		p = defaultConfPath
	}
	_, err := Parse(r.conf, p)
	if err != nil {
		return err
	}

	return nil
}

// Unset

func (r *ReplicationDriver) Unset() error {
	// TODO
	return nil
}

// Create replication

func (r *ReplicationDriver) CreateReplication(opt *pb.CreateReplicationOpts) (*model.ReplicationSpec, error) {

	volumename := opensdsPrefix + opt.PrimaryVolumeId

	cephclient := r.CephClient()

	backupclient := r.BackupClient()

	cephenablesession, err := cephclient.NewSession()
	if err != nil {
		log.Error(" failed to start cephclient session ", err)
	}

	// Activate the exclusive-lock and journaling feature on volume.

	cephenablesession.Run("rbd  feature enable rbd/" + volumename + " exclusive-lock,journaling")
	defer cephenablesession.Close()

	cephrbdinstall, err := cephclient.NewSession()

	// Installing rbd-mirror on local cluster

	if err := cephrbdinstall.Run("apt install -y rbd-mirror"); err != nil {
		log.Error("failed to install rbd-mirror: " + err.Error())

	}
	defer cephrbdinstall.Close()

	backupsession, err := backupclient.NewSession()
	if err != nil {
		log.Error(" failed to start backupclient session ", err)
	}

	// Installing rbd-mirror on remote cluster

	if err := backupsession.Run("apt install -y rbd-mirror"); err != nil {
		log.Error("failed to install rbd-mirror: " + err.Error())

	}
	defer backupsession.Close()

	cephauthsession, err := cephclient.NewSession()
	if err != nil {
		log.Error(" failed to start cephclient session ", err)
	}

	// Create a key on ceph cluster which can access (rwx) the pool.

	if err := cephauthsession.Run("ceph auth get-or-create client.ceph mon 'allow r' osd 'allow class-read object_prefix rbd_children, allow rwx pool=rbd' -o /etc/ceph/ceph.client.ceph.keyring"); err != nil {
		log.Error("failed to create a key on ceph cluster: " + err.Error())
	}
	defer cephauthsession.Close()

	backupauthsession, error := backupclient.NewSession()

	if error != nil {

		log.Error(" failed to start backupclient session ", error)

	}

	// Create a key on remote ceph clusters which can access (rwx) the pool.

	if err := backupauthsession.Run("ceph --cluster remote auth get-or-create client.remote mon 'allow r' osd 'allow class-read object_prefix rbd_children, allow rwx pool=rbd' -o /etc/ceph/remote.client.remote.keyring"); err != nil {
		log.Error("failed to create a key on remote ceph cluster: " + err.Error())
	}
	defer backupauthsession.Close()

	cephenablesession, err = cephclient.NewSession()
	if err != nil {
		log.Error(" failed to start cephclient session ", err)
	}
	// Enable volume mirroring at ceph Cluster

	if err := cephenablesession.Run("rbd mirror pool enable rbd image"); err != nil {
		log.Error("failed to enable volume mirroring at ceph cluster: " + err.Error())
	}
	defer cephenablesession.Close()

	backupenablesession, error := backupclient.NewSession()
	if error != nil {
		log.Error(" failed to start backupclient session ", error)
	}
	// Enable volume mirroring at remote Cluster

	if err := backupenablesession.Run("rbd --cluster remote mirror pool enable rbd image"); err != nil {
		log.Error("failed to enable volume mirroring at remote cluster: " + err.Error())
	}
	defer backupenablesession.Close()

	cephscpsession, err := cephclient.NewSession()
	if err != nil {
		log.Error(" failed to start cephclient session ", err)
	}

	// Copy the keys and configs of ceph cluster to remote cluster. The rbd-mirror in the primary cluster requires the key from the secondary and vice versa.

	cmd := "scp " + r.conf.FilePath + "ceph.client.ceph.keyring " + r.conf.FilePath + "ceph.conf root@" + r.conf.Ipaddresspeer + ":" + r.conf.FilePath
	if err := cephscpsession.Run(cmd); err != nil {
		log.Error("failed to copy the keys and configs of ceph cluster to remote cluster: " + err.Error())
	}
	defer cephscpsession.Close()

	backupscpsession, error := backupclient.NewSession()
	if error != nil {
		log.Error(" failed to start backupclient session ", error)
	}

	// Copy the keys and configs of remote cluster to ceph cluster. The rbd-mirror in the secondary cluster requires the key from the primary and vice versa.

	peercmd := "scp " + r.conf.FilePath + "remote.client.remote.keyring " + r.conf.FilePath + "remote.conf root@" + r.conf.IPaddresshost + ":" + r.conf.FilePath

	if err := backupscpsession.Run(peercmd); err != nil {
		log.Error("failed to copy the keys and configs of remote cluster to ceph cluster: " + err.Error())
	}
	defer backupscpsession.Close()

	cephrbdmirrorsession, err := cephclient.NewSession()
	if err != nil {
		log.Error(" failed to start cephclient session ", err)
	}
	// Enable/start the ceph-rbd-mirror at Ceph cluster
	if err := cephrbdmirrorsession.Run("systemctl start ceph-rbd-mirror@ceph"); err != nil {
		log.Error("failed to runceph-rbd-mirror at ceph cluster: " + err.Error())
	}
	defer cephrbdmirrorsession.Close()

	backuprbdmirrorsession, error := backupclient.NewSession()
	if error != nil {
		log.Error(" failed to start backupclient session ", error)
	}
	// Enable/start the ceph-rbd-mirror at remote cluster
	if err := backuprbdmirrorsession.Run("systemctl start ceph-rbd-mirror@remote"); err != nil {
		log.Error("failed to start ceph-rbd-mirror at remote cluster  : " + err.Error())
	}
	defer backuprbdmirrorsession.Close()

	cephpeeraddsession, err := cephclient.NewSession()
	if err != nil {
		log.Error(" failed to start cephclient session ", err)
	}

	//	Add the remote cluster as a peer on ceph cluster
	err = cephpeeraddsession.Run("rbd mirror pool peer add rbd client.remote@remote")
	if err != nil {
	}

	defer cephpeeraddsession.Close()

	backuppeersession, err := backupclient.NewSession()
	if err != nil {
		log.Error(" failed to start backupclient session ", err)
	}
	//	Add the ceph cluster as a peer on remote cluster
	if err := backuppeersession.Run("rbd --cluster remote mirror pool peer add rbd client.ceph@ceph"); err != nil {
	}
	defer backuppeersession.Close()
	PrimaryVolumeId := opensdsPrefix + opt.PrimaryVolumeId
	SecondaryVolumeId := opensdsPrefix + opt.SecondaryVolumeId
	PoolId := opt.PoolId
	AvailbilityZone := opt.AvailabilityZone
	profileid := opt.ProfileId

	additionalCephData := map[string]string{
		"PrimaryIP": r.conf.IPaddresshost,
	}

	additionalBackupData := map[string]string{
		"RemoteIP": r.conf.Ipaddresspeer,
	}

	return &model.ReplicationSpec{
		PrimaryVolumeId:                PrimaryVolumeId,
		SecondaryVolumeId:              SecondaryVolumeId,
		PoolId:                         PoolId,
		ProfileId:                      profileid,
		AvailabilityZone:               AvailbilityZone,
		PrimaryReplicationDriverData:   additionalCephData,
		SecondaryReplicationDriverData: additionalBackupData,
	}, nil

}

// Delete replication
func (r *ReplicationDriver) DeleteReplication(opt *pb.DeleteReplicationOpts) error {

	cephclient := r.CephClient()

	backupclient := r.BackupClient()

	cephsession, err := cephclient.NewSession()
	if err != nil {
		log.Error(" failed to start cephclient session ", err)
	}
	// Stop the ceph-rbd-mirror at ceph cluster
	if err := cephsession.Run("systemctl stop ceph-rbd-mirror@ceph"); err != nil {
		logs.Error("failed to stop ceph-rbd-mirror at ceph cluster", err)
		return err
	}
	defer cephsession.Close()

	backupsession, err := backupclient.NewSession()
	if err != nil {
		log.Error(" failed to start backupclient session ", err)
	}
	// Stop the ceph-rbd-mirror at remote cluster
	if err := backupsession.Run("systemctl stop ceph-rbd-mirror@remote"); err != nil {
		logs.Error("failed to stop ceph-rbd-mirror at remote cluster", err)
		return err
	}

	defer backupsession.Close()

	return nil
}

// Start Replication
func (r *ReplicationDriver) EnableReplication(opt *pb.EnableReplicationOpts) error {

	cephclient := r.CephClient()

	cephsession, err := cephclient.NewSession()
	if err != nil {
		log.Error(" failed to start cephclient session ", err)
	}

	volumename := opensdsPrefix + opt.PrimaryVolumeId

	// Enable image mirroring of the volume on ceph cluster(Primary cluster)

	cmd := "rbd mirror image enable rbd/" + volumename + " --pool rbd"

	if err := cephsession.Run(cmd); err != nil {
		log.Error("failed to enable volume: " + err.Error())
		return err

	}

	defer cephsession.Close()

	return nil
}

// Stop Replication
func (r *ReplicationDriver) DisableReplication(opt *pb.DisableReplicationOpts) error {

	cephclient := r.CephClient()

	backupclient := r.BackupClient()

	volumename := opensdsPrefix + opt.PrimaryVolumeId

	cephdemotesession, err := cephclient.NewSession()
	if err != nil {
		log.Error(" failed to start cephclient session ", err)
	}

	// volume is deactive on the ceph (primary) cluster
	demotecmd := "rbd mirror image demote rbd/" + volumename
	if err := cephdemotesession.Run(demotecmd); err != nil {
		log.Error("failed to demote volume at primary cluster : " + err.Error())
		return err
	}

	defer cephdemotesession.Close()

	time.Sleep(10 * time.Second)

	backuppromotesession, err := backupclient.NewSession()
	if err != nil {
		log.Error(" failed to start backupclient session ", err)
	}
	// volume is active on the remote (backup) cluster
	promotecmd := "rbd mirror image promote rbd/" + volumename + " --cluster remote"
	if err := backuppromotesession.Run(promotecmd); err != nil {
		log.Error("failed to promote volume on remote cluster : " + err.Error())
		return err

	}

	defer backuppromotesession.Close()

	time.Sleep(10 * time.Second)

	cephdisablesession, err := cephclient.NewSession()
	if err != nil {
		log.Error(" failed to start cephclient session ", err)
	}

	// Disable image mirroring of the volume on ceph cluster
	cmd := "rbd mirror image disable rbd/" + volumename + " --force"
	if err := cephdisablesession.Run(cmd); err != nil {
		log.Error("failed to disable volume rbd-mirror: " + err.Error())
		return err

	}

	defer cephdisablesession.Close()
	time.Sleep(10 * time.Second)

	backupsnapshot, err := backupclient.NewSession()
	if err != nil {
		log.Error(" failed to start backupclient session ", err)
	}
	// Create snap of volume at backup cluster

	snapcmd := "rbd snap create rbd/" + volumename + "@" + volumename + " --cluster remote"
	if err := backupsnapshot.Run(snapcmd); err != nil {
		log.Error("failed to create snap on backup cluster : " + err.Error())
		return err
	}

	defer backupsnapshot.Close()

	return nil
}

func (r *ReplicationDriver) FailoverReplication(opt *pb.FailoverReplicationOpts) error {
	return nil
}
