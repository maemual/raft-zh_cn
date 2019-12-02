package main

import (
	"fmt"
	"github.com/pingcap/log"
	"math/rand"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

/* 实现简单的Raft协议，用于学习。这是第一个版本，后续实现复杂场景的raft协议
   参考：https://blog.csdn.net/s15738841819/article/details/84286276
*/

const RAFTCOUNT = 3

type Leader struct {
	Term      int    // 任期
	LeaderId  int
}

// 用于存储leader对象
// 最初任期为0，-1代表没有leader
var leader = Leader{0, -1}

// 声明raft节点类型
type Raft struct {
	mu                  sync.Mutex    // 锁
	me                  int           // 节点编号
	currentTerm         int
	votedFor            int       // 为谁投票
	state               int       // 0: follower, 1: candidate, 2: leader
	lastMessageTime     int64     // 发送最后一条消息的时间戳
	currentLeader       int       // 当前节点的领导
	messageChan         chan bool // 消息通道
	electChan           chan bool // 选举通道
	heartbeatChan       chan bool // 心跳信号
	returnHeartbeatChan chan bool // 返回心跳信号
	timeout             int       // 超时时间
}


func main() {
	// 流程： 创建3个节点，follower状态，若出现candidate状态，则开始投票，产生leader

	for i := 0; i < RAFTCOUNT; i++ {

		createNode(i)    // 创建节点
		fmt.Println("当前node: ", i)
	}

	//对raft结构体实现rpc注册
	rpc.Register(new(Raft))
	rpc.HandleHTTP()
	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal(err.Error())
	}

	//防止选举没完成，main结束了
	for {;
	}
}


func createNode(me int) *Raft{
	raft := &Raft{}
	raft.me = me
	raft.votedFor = -1    // 不给任何人投票
	raft.state = 0
	raft.timeout = 0
	raft.currentLeader = -1
	raft.setTerm(0)    // 设置任期

	// 初始化通道
	raft.electChan = make(chan bool)
	raft.messageChan = make(chan bool)
	raft.heartbeatChan = make(chan bool)
	raft.returnHeartbeatChan = make(chan bool)

	// 随机种子
	rand.Seed(time.Now().UnixNano())

	// 选举逻辑
	go raft.election()

	// 心跳检测
	go raft.sendLeaderHeartbeat()

	return raft
}

func (raft *Raft) setTerm(term int) {
	raft.currentTerm = term
}

func (raft *Raft) election()  {
	var result bool    // 设置标签
	for {
		timeout := randRange(150, 300)

		// 设置每个节点最后一条信息的时间
		raft.lastMessageTime = millisecond()

		select {
		case <- time.After(time.Duration(timeout) * time.Millisecond):
			fmt.Println("当前节点的状态为：", raft.state)
		}
		result = false

		// 选leader, 如果选出leader, 停止循环，result设置为True
		for !result {
			result = raft.electOneRand(&leader)    // 随机选择一个为leader
		}
	}
}

// 选择leader
func (raft *Raft) electOneRand(leader *Leader) bool {
	var timeout int64 = 100
	var vote int    // 投票数量
	var triggerHeartbeat bool    // 用于判断是否开始心跳信号的方法
	last := millisecond()    // 当前时间戳对应的毫秒
	success := false     // 定义返回值

	// 转换为candidate 状态
	raft.mu.Lock()
	raft.becomeCandidate()
	raft.mu.Unlock()

	fmt.Println("start electing leader ...")
	for {
		// 遍历所有的节点进行投票
		for i := 0; i < RAFTCOUNT; i++ {
			// 遍历到不是自己，进行拉票
			if i != raft.me {
				// 其他节点，拉票
				go func () {
					// 其他节点没有领导
					if leader.LeaderId < 0 {
						// 操作选举通道
						raft.electChan <- true
					}
				} ()
			}
		}

		// 设置投票数量
		vote = 0
		triggerHeartbeat = false
		// 遍历所有节点进行选举
		for i := 0; i < RAFTCOUNT; i++ {
			// 计算投票的数量
			select {
			case ok := <-raft.electChan:
				if ok {
					// 投票数量 +1
					vote++
					// 返回值success, 大于总票数的一半
					success = vote > RAFTCOUNT / 2
					// 若成为leader状态
					// 如果票数大于一半， 且未发出心跳信号
					if success && !triggerHeartbeat {
						// 选举成功
						// 发出心跳信号
						triggerHeartbeat = true

						raft.mu.Lock()
						raft.becomeLeader()    // 真正成为leader
						raft.mu.Unlock()

						// 由leader向其他节点发送心跳信号
						// 开启心跳信号的通道
						raft.heartbeatChan <- true
						fmt.Println(raft.me, "号 节点成为了leader")
						fmt.Println("leader发送心跳信号")
					}

				}
			}
		}

		// 间隔时间小于100 毫秒 左右
		// 若不超时，且票数 大于一半，且当前有领导
		if (timeout + last <millisecond() || (vote >= RAFTCOUNT / 2 || raft.currentLeader > -1)) {
			// 结束循环
			break
		} else {
			// 没有选出leader
			select {
			case <- time.After(time.Duration(10) * time.Millisecond):
			}
		}
	}
	return success
}

// 成为 candidate状态
func (raft *Raft) becomeCandidate() {
	raft.state = 1
	raft.setTerm(raft.currentTerm + 1)    // 节点任期 +1
	raft.votedFor = raft.me    // 设置为哪个节点投票
	raft.currentLeader = -1    // 当前没有leader
}


func (raft *Raft) becomeLeader() {
	raft.state = 2
	raft.currentLeader = raft.me
}

func (raft *Raft) sendLeaderHeartbeat() {
	for {
		select {
		case <- raft.heartbeatChan:
			// 给leader 返回确认信号
			raft.sendAppendEntriesImpl()
		}
	}
}

// 返回给leader的确认信号
func (raft *Raft) sendAppendEntriesImpl() {
	// 判断当前是否是leader节点
	if raft.currentLeader == raft.me {
		// 声明返回确认信号的节点个数
		var success_count = 0

		// 设置返回确认信号的子节点
		for i := 0; i < RAFTCOUNT; i++ {
			// 若当前不是本节点
			if i != raft.me {
				go func() {
					// RPC
					rp, err := rpc.DialHTTP("tcp", "127.0.0.1:8080")
					if err != nil {
						log.Fatal(err.Error())
					}

					// 接受服务器发来的消息
					var ok = false
					er := rp.Call("Raft.Communication", Param{"hello"}, &ok)
					if er != nil {
						log.Fatal(er.Error())
					}

					if ok {
						// rpc通讯的情况下，子节点有返回
						raft.heartbeatChan <- true
					}
				}()
			}
		}

		// 计算返回确认信号的子节点，若子节点个数>raftCount/2，则校验成功
		for i := 0; i < RAFTCOUNT; i++ {
			select {
			case ok := <- raft.returnHeartbeatChan:
				if ok {
					//记录返回确认信号的子节点的个数
					success_count++
					if success_count > RAFTCOUNT/2 {
						fmt.Println("投票选举成功，校验心跳信号成功")
						log.Fatal("程序结束")
					}
				}
			}
		}
	}
}

// 生成随机值
func randRange(min, max int64) int64 {
	// 用于心跳信号的时间
	return rand.Int63n(max - min) + min
}

// 获取当前时间的毫秒数
func millisecond() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}


//分布式通信
type Param struct {
	Msg string
}


//等待客户端消息
func (r *Raft) Communication(p Param, a *bool) error {
	fmt.Println(p.Msg)
	*a = true
	return nil
}