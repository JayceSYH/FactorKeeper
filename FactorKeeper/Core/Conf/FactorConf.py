class FactorConf(object):
    FACTOR_INIT_VERSION = "INIT_VERSION"
    GROUP_FACTOR_PREFIX = "FACTOR_KEEPER_GROUP_FACTOR_"
    FACTOR_LENGTH = 4740
    
    @staticmethod
    def get_group_factor_name(factors):
        factors = sorted(factors)
        return FactorConf.GROUP_FACTOR_PREFIX + "#".join(factors)
