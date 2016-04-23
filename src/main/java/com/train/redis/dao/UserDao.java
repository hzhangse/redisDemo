package com.train.redis.dao;

import java.util.List;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.BoundHashOperations;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import com.train.redis.User;

@Component
public class UserDao extends AbstractBaseRedisDao<String, User> implements
		IUserDao {

	/**
	 * 新增 <br>
	 * ------------------------------<br>
	 * 
	 * @param user
	 * @return
	 */
	public boolean add(final User user) {
		boolean result = redisTemplate.execute(new RedisCallback<Boolean>() {
			public Boolean doInRedis(RedisConnection connection)
					throws DataAccessException {
				RedisSerializer<String> serializer = getRedisSerializer();
				byte[] key = serializer.serialize(user.getId());
				byte[] name = serializer.serialize(user.getName());
				return connection.setNX(key, name);
			}
		});
		return result;
	}

	/**
	 * 批量新增 使用pipeline方式 <br>
	 * ------------------------------<br>
	 * 
	 * @param list
	 * @return
	 */
	public boolean add(final List<User> list) {
		Assert.notEmpty(list);
		boolean result = redisTemplate.execute(new RedisCallback<Boolean>() {
			public Boolean doInRedis(RedisConnection connection)
					throws DataAccessException {
				RedisSerializer<String> serializer = getRedisSerializer();
				for (User user : list) {
					byte[] key = serializer.serialize(user.getId());
					byte[] name = serializer.serialize(user.getName());
					connection.setNX(key, name);
				}
				return true;
			}
		}, false, true);
		return result;
	}

	/**
	 * 删除 <br>
	 * ------------------------------<br>
	 * 
	 * @param key
	 */
	public void delete(final String key) {

		final byte[] bkey = this.getRedisSerializer().serialize(key);

		redisTemplate.execute(new RedisCallback<Object>() {
			public Object doInRedis(RedisConnection connection) {
				connection.del(bkey);
				return null;
			}
		});
	}

	/**
	 * 删除多个 <br>
	 * ------------------------------<br>
	 * 
	 * @param keys
	 */
	public void delete(List<String> keys) {

		redisTemplate.delete(keys);

	}

	/**
	 * 修改 <br>
	 * ------------------------------<br>
	 * 
	 * @param user
	 * @return
	 */
	public boolean update(final User user) {
		String key = user.getId();
		if (get(key) == null) {
			throw new NullPointerException("数据行不存在, key = " + key);
		}
		boolean result = redisTemplate.execute(new RedisCallback<Boolean>() {
			public Boolean doInRedis(RedisConnection connection)
					throws DataAccessException {
				RedisSerializer<String> serializer = getRedisSerializer();
				byte[] key = serializer.serialize(user.getId());
				byte[] name = serializer.serialize(user.getName());
				connection.set(key, name);
				return true;
			}
		});
		return result;
	}

	/**
	 * 通过key获取 <br>
	 * ------------------------------<br>
	 * 
	 * @param keyId
	 * @return
	 */
	public User get(final String keyId) {
		User result = redisTemplate.execute(new RedisCallback<User>() {
			public User doInRedis(RedisConnection connection)
					throws DataAccessException {
				RedisSerializer<String> serializer = getRedisSerializer();
				byte[] key = serializer.serialize(keyId);
				byte[] value = connection.get(key);
				if (value == null) {
					return null;
				}
				String name = serializer.deserialize(value);
				return new User(keyId, name, null);
			}
		});
		return result;
	}

	public void hmsetUser(final User user) {
		this.redisTemplate.execute(new RedisCallback<User>() {

			public User doInRedis(RedisConnection connection)
					throws DataAccessException {
				RedisSerializer<String> serializer = getRedisSerializer();
				byte[] key = serializer.serialize(user.getId());
				BoundHashOperations<String, byte[], byte[]> boundHashOperations = redisTemplate
						.boundHashOps(user.getId());
				boundHashOperations.put(redisTemplate.getStringSerializer()
						.serialize("name"), redisTemplate.getStringSerializer()
						.serialize(user.getName()));
				;
				boundHashOperations.put(redisTemplate.getStringSerializer()
						.serialize("password"), redisTemplate
						.getStringSerializer().serialize(user.getPassword()));
				;
				connection.hMSet(key, boundHashOperations.entries());
				return null;
			}

		});

	}

	public User hmgetUser(final String uid) {
		return this.redisTemplate.execute(new RedisCallback<User>() {

			public User doInRedis(RedisConnection connection)
					throws DataAccessException {
				 byte[] key = redisTemplate.getStringSerializer().serialize(  
		                     uid);  
		            if (connection.exists(key)) {  
		                List<byte[]> value = connection.hMGet(  
		                        key,  
		                        redisTemplate.getStringSerializer().serialize(  
		                                "name"),  
		                        redisTemplate.getStringSerializer().serialize(  
		                                "password"));  
		                User user = new User();  
		                String name = redisTemplate.getStringSerializer()  
		                        .deserialize(value.get(0));  
		                user.setName(name);
		                String password = redisTemplate.getStringSerializer()  
		                        .deserialize(value.get(1));  
		                user.setPassword(password);
		                user.setId(uid);
		  
		                return user;  
		            }  
		            return null;  
			}

		});
	}
}