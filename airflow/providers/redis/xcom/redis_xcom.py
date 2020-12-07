import json
import logging
from typing import Any
import uuid

from airflow.configuration import conf
from airflow.models.xcom import BaseXCom
from airflow.providers.redis.hooks.redis import RedisHook

log = logging.getLogger(__name__)
redis_conn_id = conf.get('xcom', 'redis_conn_id', fallback=RedisHook.default_conn_name)
redis_hook = RedisHook(redis_conn_id=redis_conn_id)


class RedisXCom(BaseXCom):

    @classmethod
    def delete(cls, xcom):
        """Delete XCom value from Redis"""
        result = redis_hook.get_conn().delete(xcom.value)
        log.debug("Result of deleting key to Redis %s", result)

    @staticmethod
    def serialize_value(value: Any):
        """Serialize the data as JSON, store into Redis & return the key"""
        val = json.dumps(value).encode('UTF-8')
        key = uuid.uuid4().hex

        log.debug('Setting XCom key %s to Redis', key)
        result = redis_hook.get_conn().set(key, val)
        log.debug('Result of publishing to Redis %s', result)
        return BaseXCom.serialize_value(key)

    @staticmethod
    def deserialize_value(result: "XCom") -> Any:
        result = redis_hook.get_conn().get(result.value.decode().strip('"'))
        return json.loads(result.decode('UTF-8')) if result is not None \
            else '** XCom not found in Redis **'
