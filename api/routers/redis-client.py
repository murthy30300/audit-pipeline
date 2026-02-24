import redis

def get_redis():

    return redis.Redis(

        host="redis",   # docker service name

        port=6379,

        decode_responses=True

    )