import misc,diff,pg,syncschema
# don't import pgmock and stubredis -- they're only useful for test mode or nonstandard env (i.e. stubredis on windows)
# don't import redismodel by default -- it has an extra msgpack dependency
__version__ = '0.0.6'
