package IBM

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/ibm-messaging/mq-golang/ibmmq"
)

type MqInterface interface {
	MqPutSingle()
	MqGetSingle()
}

type MqStruct struct {
	QueueManager string
	Queue        string
	Message      string
	Channel      string
	Connection   string
}

func Connect(Qmg string, Queue string, ChannelName string) MqInterface {
	mq := new(MqStruct)
	mq.QueueManager = Qmg
	mq.Queue = Queue
	mq.Channel = ChannelName
	return mq
}

var qMgrObject ibmmq.MQObject
var qObject ibmmq.MQObject

func (m MqStruct) Push() {
	qMgrName := m.QueuManager
	qName := m.Queue

	qMgrObject, err := ibmmq.Conn(qMgrName)
	if err == nil {
		defer disc(qMgrObject)
	}

	// Open the queue
	if err == nil {
		mqod := ibmmq.NewMQOD()
		openOptions := ibmmq.MQOO_OUTPUT
		mqod.ObjectType = ibmmq.MQOT_Q
		mqod.ObjectName = qName

		qObject, err = qMgrObject.Open(mqod, openOptions)
		if err != nil {
		} else {
			defer close(qObject)
		}
	}

	// PUT a message to the queue
	if err == nil {

		putmqmd := ibmmq.NewMQMD()
		pmo := ibmmq.NewMQPMO()
		pmo.Options = ibmmq.MQPMO_NO_SYNCPOINT
		putmqmd.Format = ibmmq.MQFMT_STRING
		msgData := m.msg
		buffer := []byte(msgData)
		putmqmd.CorrelId = []byte(m.corlationID)
		err = qObject.Put(putmqmd, pmo, buffer)

		if err != nil {
			fmt.Println(err)
		} else {
			m.put = true
			m.msgID = hex.EncodeToString(putmqmd.MsgId)
		}
	}

	mqret := 0
	if err != nil {
		mqret = int((err.(*ibmmq.MQReturn)).MQCC)
	}
	m.result = mqret
}

func disconnect(qMgrObject ibmmq.MQQueueManager) error {
	err := qMgrObject.Disc()
	if err == nil {

		fmt.Printf("Disconnected from queue manager %s\n", qMgrObject.Name)
	} else {
		fmt.Println(err)
	}
	return err
}

func (m MqStruct) Pull() {

	qMgrName := m.QueuManager
	qName := m.Queue

	qMgrObject, err := ibmmq.Conn(qMgrName)
	if err != nil {
		m.connect = false
	} else {
		m.connect = true
		defer disc(qMgrObject)
	}

	if err == nil {

		mqod := ibmmq.NewMQOD()
		openOptions := ibmmq.MQOO_INPUT_EXCLUSIVE
		mqod.ObjectType = ibmmq.MQOT_Q
		mqod.ObjectName = qName

		qObject, err = qMgrObject.Open(mqod, openOptions)
		if err != nil {
			m.connect = false
		} else {
			m.connect = true
			defer close(qObject)
		}
	}

	var datalen int
	getmqmd := ibmmq.NewMQMD()
	gmo := ibmmq.NewMQGMO()
	gmo.Options = ibmmq.MQGMO_NO_SYNCPOINT
	gmo.Options |= ibmmq.MQGMO_WAIT
	gmo.WaitInterval = 3 * 1000 // The WaitInterval is in milliseconds
	useGetSlice := true
	if useGetSlice {
		buffer := make([]byte, 0, 1024)
		buffer, datalen, err = qObject.GetSlice(getmqmd, gmo, buffer)

		if err != nil {
			fmt.Println(err)
			mqret := err.(*ibmmq.MQReturn)
			if mqret.MQRC == ibmmq.MQRC_NO_MSG_AVAILABLE {
				err = nil
				m.msg = "NOTMESSAGEAVILABLE"
			}
		} else {

			m.msg = strings.TrimSpace(string(buffer))
		}
	} else {

		buffer := make([]byte, 1024)
		datalen, err = qObject.Get(getmqmd, gmo, buffer)

		if err != nil {
			fmt.Println(err)
			mqret := err.(*ibmmq.MQReturn)
			if mqret.MQRC == ibmmq.MQRC_NO_MSG_AVAILABLE {
				err = nil
				m.msg = "NOTMESSAGEAVILABLE"
			}
		} else {

			fmt.Printf("Got message of length %d: ", datalen)
			m.msg = strings.TrimSpace(string(buffer[:datalen]))
		}
	}

	mqret := 0
	if err != nil {
		mqret = int((err.(*ibmmq.MQReturn)).MQCC)
	}
	//	return mqret
	fmt.Println(mqret)

}

func close(object ibmmq.MQObject) error {
	err := object.Close(0)
	if err == nil {
		fmt.Println("Closed queue")
	} else {
		fmt.Println(err)
	}
	return err
}
