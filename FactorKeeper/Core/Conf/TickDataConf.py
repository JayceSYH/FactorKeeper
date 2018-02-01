class TickDataConf(object):
    TICK_LENGTH = 4740

    STOCK_VIEW_SUFFIX = ".VIEW"

    @classmethod
    def is_stock_view(cls, stock_code):
        return stock_code.endswith(cls.STOCK_VIEW_SUFFIX)
