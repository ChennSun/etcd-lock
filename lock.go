package etcd

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"time"
)

func Lock(ctx context.Context, key string, fn func()) error {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 60 * time.Second,
		Username:    "root",
		Password:    "root",
	})
	if err != nil {
		return err
	}
	s, _ := concurrency.NewSession(cli)
	defer s.Close()
	var mutex = concurrency.NewMutex(s, key)
	err = mutex.Lock(ctx)
	if err != nil {
		return err
	}
	defer mutex.Unlock(ctx)
	fn()
	return nil
}

/**
第一版：
通过key createReversion判断锁是否被占用
通过for循环多次执行txn方式尝试加锁，多次请求etcd，成本较高，锁竞争严重
*/
func Lock_v1(ctx context.Context, key string, fn func()) error {
	// 创建rpc client
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 60 * time.Second,
		Username:    "root",
		Password:    "root",
	})
	if err != nil {
		return err
	}
	// 创建租约, 租约过期时间太短，可能导致keep alive失败
	l, err := cli.Grant(ctx, 10)
	if err != nil {
		return err
	}
	// 销毁租约，撤销后续租会自动停止
	// 租约被销毁后，与租约绑定的key也会被销毁
	defer cli.Revoke(ctx, l.ID)
	// 开启续租
	responseChan, err := cli.KeepAlive(ctx, l.ID)
	if err != nil {
		return err
	}
	go func() {
		// 应答，防止chan被塞满
		// 租约撤销后，chan会被close,for range退出
		for range responseChan {
		}
	}()
	// 开始尝试加锁, 事务保证原子性
	var lockRes = false
	for !lockRes {
		txn := cli.Txn(ctx)
		resp, err := txn.
			// 如果key不存在，key对应的"CreateRevision"字段也不存在，所以如果为0，则表示key不存在
			If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
			Then(clientv3.OpPut(key, "", clientv3.WithLease(l.ID))).
			Commit()
		if err != nil {
			return fmt.Errorf("acquire lock failed: %v", err)
		}
		// 加锁失败
		if !resp.Succeeded {
			time.Sleep(time.Millisecond * 10)
			continue
		}
		lockRes = true
	}

	// 执行业务逻辑
	fn()
	return nil
}

/**
第二版：
使用watch方案监控锁是否释放，避免多次调用etcd
惊群效应严重
*/
func Lock_v2(ctx context.Context, key string, fn func()) error {
	// 创建rpc client
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 60 * time.Second,
		Username:    "root",
		Password:    "root",
	})
	if err != nil {
		return err
	}
	// 创建租约, 租约过期时间太短，可能导致keep alive失败
	l, err := cli.Grant(ctx, 20)
	if err != nil {
		return err
	}
	// 销毁租约，撤销后续租会自动停止
	// 租约被销毁后，与租约绑定的key也会被销毁
	defer cli.Revoke(ctx, l.ID)
	// 开启续租
	responseChan, err := cli.KeepAlive(ctx, l.ID)
	if err != nil {
		return err
	}
	go func() {
		// 应答，防止chan被塞满
		// 租约撤销后，chan会被close,for range退出
		for range responseChan {
		}
	}()
	// 开始尝试加锁, 事务保证原子性
lock:
	txn := cli.Txn(ctx)
	resp, err := txn.
		// 如果key不存在，key对应的"CreateRevision"字段也不存在，所以如果为0，则表示key不存在
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, "", clientv3.WithLease(l.ID))).
		Commit()
	if err != nil {
		return err
	}
	// 加锁失败
	if !resp.Succeeded {
		wChan := cli.Watch(ctx, key)
		for e := range wChan {
			for _, ev := range e.Events {
				if ev.Type == mvccpb.DELETE {
					goto lock
				}
			}
		}
	}

	// 执行业务逻辑
	fn()
	return nil
}

/*
第三版
每个key监控之前前一个版本的key，避免惊群效应
不以put key成功作为加锁成功依据，如果key之前没有更早版本的key 存在时，才表示加锁成功
会形成一条排队等待的链条
*/
func Lock_v3(ctx context.Context, prefix string, fn func()) error {
	prefix = prefix + "/"
	// 创建rpc client
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 60 * time.Second,
		Username:    "root",
		Password:    "root",
	})
	if err != nil {
		return err
	}
	// 创建租约, 租约过期时间太短，可能导致keep alive失败
	l, err := cli.Grant(ctx, 20)
	if err != nil {
		return err
	}
	// 销毁租约，撤销后续租会自动停止
	// 租约被销毁后，与租约绑定的key也会被销毁
	defer cli.Revoke(ctx, l.ID)
	// 开启续租
	responseChan, err := cli.KeepAlive(ctx, l.ID)
	if err != nil {
		return err
	}
	go func() {
		// 应答，防止chan被塞满
		// 租约撤销后，chan会被close,for range退出
		for range responseChan {
		}
	}()

	key := fmt.Sprintf("%s%d", prefix, l.ID)
	// key是否已经存在
	cmp := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	// 写入key
	opPut := clientv3.OpPut(key, "", clientv3.WithLease(l.ID))
	// 获取key相关元数据
	opGet := clientv3.OpGet(key)
	// 获取最早创建的一个key相关信息，该key为锁的占有者
	getOwner := clientv3.OpGet(prefix, clientv3.WithFirstCreate()...)
	// 如果key不存在，则创建key并返回最早创建的key信息，如果存在则返回key的数据
	// etcdctl get test -w fields --user=root --password=root --prefix=true --sort-by=CREATE --order=ascend --limit=1
	resp, err := cli.Txn(ctx).If(cmp).Then(opPut, getOwner).Else(opGet, getOwner).Commit()
	if err != nil {
		return err
	}
	// 由于事务一次性执行多条op语句, 然后返回执行结果，返回的response为interface类型的切片， 需要使用相关方法进行断言；
	owner := resp.Responses[1].GetResponseRange().Kvs

	var currentVersion int64
	if resp.Succeeded {
		if len(owner) == 0 || owner[0].CreateRevision == resp.Header.Revision {
			fn()
			return nil
		}
		currentVersion = resp.Header.Revision
	} else {
		currentVersion = resp.Responses[0].GetResponseRange().Kvs[0].CreateRevision
	}
	// 等待前一个key删除
	if err := waitDelete(ctx, cli, prefix, currentVersion-1); err != nil {
		return err
	}
	// 执行业务逻辑
	fn()
	return nil
}

func waitDelete(ctx context.Context, cli *clientv3.Client, prefix string, preVersion int64) error {
	// 查询前一个key
	// etcdctl get test -w fields --user=root --password=root --prefix=true ---sort-by=MODIFY -order=descend --limit=1
	opt := append(clientv3.WithLastRev(), clientv3.WithMaxCreateRev(preVersion))
	resp, err := cli.Get(ctx, prefix, opt...)
	if err != nil {
		return err
	}
	if len(resp.Kvs) == 0 {
		return nil
	}
	lastKey := string(resp.Kvs[0].Key)
	// etcdctl watch test --user=root --password=root --rev=1
	wChan := cli.Watch(ctx, lastKey, clientv3.WithRev(resp.Header.Revision))
	for e := range wChan {
		for _, ev := range e.Events {
			if ev.Type == mvccpb.DELETE {
				return nil
			}
		}
	}
	return nil
}
