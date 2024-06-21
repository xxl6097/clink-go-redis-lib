package redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/xxl6097/go-glog/glog"
	"strings"
	"time"
)

var Redis = redisUtil{}

// redisUtil Redis操作工具类
type redisUtil struct{}

// stringToLines string拆分多行
func stringToLines(s string) (lines []string, err error) {
	scanner := bufio.NewScanner(strings.NewReader(s))
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	err = scanner.Err()
	return
}

// stringToKV string拆分key和val
func stringToKV(s string) (string, string) {
	ss := strings.Split(s, ":")
	if len(ss) < 2 {
		return s, ""
	}
	return ss[0], ss[1]
}

// Info Redis服务信息
func (ru redisUtil) Info(sections ...string) (res map[string]string) {
	infoStr, err := client.Info(context.Background(), sections...).Result()
	res = map[string]string{}
	if err != nil {
		glog.Errorf("redisUtil.Info err: err=[%+v]", err)
		return res
	}
	// string拆分多行
	lines, err := stringToLines(infoStr)
	if err != nil {
		glog.Errorf("stringToLines err: err=[%+v]", err)
		return res
	}
	// 解析成Map
	for i := 0; i < len(lines); i++ {
		if lines[i] == "" || strings.HasPrefix(lines[i], "# ") {
			continue
		}
		k, v := stringToKV(lines[i])
		res[k] = v
	}
	return res
}

// DBSize 当前数据库key数量
func (ru redisUtil) DBSize() int64 {
	size, err := client.DBSize(context.Background()).Result()
	if err != nil {
		glog.Errorf("redisUtil.DBSize err: err=[%+v]", err)
		return 0
	}
	return size
}

// Set 设置键值对
func (ru redisUtil) Set(key string, value interface{}, time time.Duration) error {
	err := client.Set(context.Background(), key, value, time).Err() //time.Duration(timeSec)*time.Second
	if err != nil {
		glog.Errorf("redisUtil.Set err: err=[%+v]", err)
		return err
	}
	return nil
}

// Get 获取key的值
func (ru redisUtil) Get(key string) (string, error) {
	res, err := client.Get(context.Background(), key).Result()
	if err != nil {
		glog.Errorf("redisUtil.Get[%s] err: err=[%+v]", key, err)
		return res, err
	}
	return res, err
}

func (ru redisUtil) Scan(key string, model interface{}) error {
	err := client.Get(context.Background(), key).Scan(model)
	return err
}

// SSet 将数据放入set缓存
func (ru redisUtil) SSet(key string, values ...interface{}) bool {
	err := client.SAdd(context.Background(), key, values...).Err()
	if err != nil {
		glog.Errorf("redisUtil.SSet err: err=[%+v]", err)
		return false
	}
	return true
}

// SGet 根据key获取Set中的所有值
func (ru redisUtil) SGet(key string) []string {
	res, err := client.SMembers(context.Background(), key).Result()
	if err != nil {
		glog.Errorf("redisUtil.SGet err: err=[%+v]", err)
		return []string{}
	}
	return res
}

// HMSet 设置key, 通过字典的方式设置多个field, value对
func (ru redisUtil) HMSet(key string, mapping map[string]string, timeSec int) bool {
	//err := client.HSet(context.Background(), key, mapping).Err()
	//if err != nil {
	//	glog.Errorf("redisUtil.HMSet err: err=[%+v]", err)
	//	return false
	//}
	//if timeSec > 0 {
	//	if !ru.Expire(key, timeSec) {
	//		return false
	//	}
	//}
	//return true

	return ru.HMSet(key, mapping, timeSec)
}

// HSet 向hash表中放入数据,如果不存在将创建
func (ru redisUtil) HSetObj(key string, field string, value interface{}, second int) bool {
	err := client.HSet(context.Background(), key, map[string]interface{}{field: value}).Err()
	if err != nil {
		glog.Errorf("redisUtil.HMSet err: err=[%+v]", err)
		return false
	}
	if second > 0 {
		if !ru.Expire(key, second) {
			return false
		}
	}

	return true
}

// HSet 向hash表中放入数据,如果不存在将创建
func (ru redisUtil) HSet(key string, field string, value string, second int) bool {
	err := client.HSet(context.Background(), key, map[string]string{field: value}).Err()
	if err != nil {
		glog.Errorf("redisUtil.HMSet err: err=[%+v]", err)
		return false
	}
	if second > 0 {
		if !ru.Expire(key, second) {
			return false
		}
	}

	return true
	//return ru.HMSet(key, map[string]string{field: value}, timeSec)
}

// HGet 获取key中field域的值
func (ru redisUtil) Test(key string, field string) string {
	res, err := client.HGet(context.Background(), key, field).Result()
	if err != nil {
		glog.Errorf("redisUtil.HGet err: err=[%+v]", err)
		return ""
	}
	return res
}

// HGet 获取key中field域的值
func (ru redisUtil) HGet(key string, field string) string {
	res, err := client.HGet(context.Background(), key, field).Result()
	if err != nil {
		glog.Errorf("redisUtil.HGet err: err=[%+v]", err)
		return ""
	}
	return res
}

func (ru redisUtil) HGetAll(key string) map[string]string {
	res, err := client.HGetAll(context.Background(), key).Result()
	if err != nil {
		glog.Errorf("redisUtil.HGet err: err=[%+v]", err)
		return nil
	}
	return res
}

// HExists 判断key中有没有field域名
func (ru redisUtil) HExists(key string, field string) bool {
	res, err := client.HExists(context.Background(), key, field).Result()
	if err != nil {
		glog.Errorf("redisUtil.HExists err: err=[%+v]", err)
		return false
	}
	return res
}

// HDel 删除hash表中的值
func (ru redisUtil) HDel(key string, fields ...string) bool {
	err := client.HDel(context.Background(), key, fields...).Err()
	if err != nil {
		glog.Errorf("redisUtil.HDel err: err=[%+v]", err)
		return false
	}
	return true
}

// Exists 判断多项key是否存在
func (ru redisUtil) Exists(keys ...string) int64 {
	fullKeys := ru.toFullKeys(keys)
	cnt, err := client.Exists(context.Background(), fullKeys...).Result()
	if err != nil {
		glog.Errorf("redisUtil.Exists err: err=[%+v]", err)
		return -1
	}
	return cnt
}

// Expire 指定缓存失效时间
func (ru redisUtil) Expire(key string, timeSec int) bool {
	err := client.Expire(context.Background(), key, time.Duration(timeSec)*time.Second).Err()
	if err != nil {
		glog.Errorf("redisUtil.Expire err: err=[%+v]", err)
		return false
	}
	return true
}

// TTL 根据key获取过期时间
func (ru redisUtil) TTL(key string) int {
	td, err := client.TTL(context.Background(), key).Result()
	if err != nil {
		glog.Errorf("redisUtil.TTL err: err=[%+v]", err)
		return 0
	}
	return int(td / time.Second)
}

// Del 删除一个或多个键
func (ru redisUtil) Del(keys ...string) bool {
	fullKeys := ru.toFullKeys(keys)
	err := client.Del(context.Background(), fullKeys...).Err()
	if err != nil {
		glog.Errorf("redisUtil.Del err: err=[%+v]", err)
		return false
	}
	return true
}

// Del 删除一个或多个键
func (ru redisUtil) Delete(keys string) (int64, error) {
	deleteid, err := client.Del(context.Background(), keys).Result()
	if err != nil {
		glog.Errorf("redisUtil.Del err: err=[%+v]", err)
		return deleteid, err
	}
	return deleteid, err
}

// toFullKeys 为keys批量增加前缀
func (ru redisUtil) toFullKeys(keys []string) (fullKeys []string) {
	for _, k := range keys {
		fullKeys = append(fullKeys, k)
	}
	return
}

func (ru redisUtil) GetEndKeys(rootkey, key string) []string {
	s := strings.Replace(key, rootkey, "", -1)
	if s == "" {
		return nil
	}
	return strings.Split(s, ":")
}

func (ru redisUtil) GetEndKey(key string) string {
	s := strings.Split(key, ":")
	return s[len(s)-1]
}

func (ru redisUtil) GetStartKey(key string) string {
	s := strings.Split(key, ":")
	return s[0]
}
func (ru redisUtil) GetPrefixKey(key, end string) string {
	return key[:len(key)-len(end)-1]
}
func (ru redisUtil) toJson(key, value string) string {
	return `{"` + key + `":` + value + `}`
}

func (ru redisUtil) GetKeys(key string) []string {
	//ru.GetKeysByScan
	//keys, _ := client.Keys(context.Background(), fmt.Sprintf("%s:*", key)).Result()
	keys, _ := ru.GetKeysByScan(context.Background(), fmt.Sprintf("%s:*", key))
	return keys
}

func (ru redisUtil) GetKeysCount(key string) int {
	//keys, err := client.Keys(context.Background(), fmt.Sprintf("%s:*", key)).Result()
	keys, err := ru.GetKeysByScan(context.Background(), fmt.Sprintf("%s:*", key))
	if err == nil {
		return len(keys)
	}
	return 0
}

func (ru redisUtil) GetKeysByScan(ctx context.Context, key string) ([]string, error) {
	iter := client.Scan(ctx, 0, key, 0).Iterator()
	keys := make([]string, 0)
	for iter.Next(ctx) {
		k := iter.Val()
		keys = append(keys, k)
	}
	if len(keys) > 0 {
		return keys, nil
	}
	return nil, errors.New("empty")
}
