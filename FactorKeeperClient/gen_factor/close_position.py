class CloseCache(object):
    def __init__(self):
        self._extreme_big = []
        self._extreme_small = []
        self._buy_close_position = {}
        self._short_close_position = {}
        self._len_extreme_small = 0
        self._len_extreme_big = 0

    def add_extreme_small(self, pos):
        self._len_extreme_small += 1
        self._extreme_small.append(pos)

    def add_extreme_big(self, pos):
        self._len_extreme_big += 1
        self._extreme_big.append(pos)

    def add_buy_close_position(self, pos, close_pos):
        self._buy_close_position[pos] = close_pos

    def get_buy_close_position(self, pos):
        return self._buy_close_position[pos]

    def add_short_close_position(self, pos, close_pos):
        self._short_close_position[pos] = close_pos

    def get_short_close_position(self, pos):
        return self._short_close_position[pos]

    @property
    def extreme_big(self):
        return self._extreme_big

    @property
    def extreme_small(self):
        return self._extreme_small

    @property
    def len_small(self):
        return self._len_extreme_small

    @property
    def len_big(self):
        return self._len_extreme_big


class ClosePositionFinder(object):
    TREND_UP = 1
    TREND_DOWN = -1
    TREND_NONE = 0

    CLOSE_THRESHOLD = lambda x: float(x) * 3.0 / 1000

    def __init__(self):
        pass

    @staticmethod
    def find_buy_close_position(price):
        pos = len(price) - 2
        close_cache = CloseCache()
        last_trend = ClosePositionFinder.TREND_NONE
        close_cache.add_short_close_position(len(price) - 1, len(price) - 1)
        close_cache.add_buy_close_position(len(price) - 1, len(price) - 1)

        while pos >= 0:
            if price[pos + 1] > price[pos]:
                if last_trend == ClosePositionFinder.TREND_DOWN:
                    close_cache.add_extreme_big(pos + 1)
                last_trend = ClosePositionFinder.TREND_UP

                buy_close_position = close_cache.get_buy_close_position(pos + 1)

                # find close position of short action
                extreme_no = 1
                short_close_position = len(price) - 1
                while extreme_no <= close_cache.len_big or extreme_no <= close_cache.len_small:
                    if extreme_no <= close_cache.len_big:
                        if price[close_cache.extreme_big[-extreme_no]] - price[pos] >= ClosePositionFinder.CLOSE_THRESHOLD(price[pos]):
                            short_close_position = close_cache.extreme_big[-extreme_no]
                            print("pos:{}".format(pos))
                            while price[short_close_position - 1] - price[pos] >= ClosePositionFinder.CLOSE_THRESHOLD(price[pos]):
                                short_close_position -= 1
                                print("pos:{0} close:{1}".format(pos, short_close_position))
                            break

                    if extreme_no <= close_cache.len_small:
                        if price[close_cache.extreme_small[-extreme_no]] <= price[pos]:
                            short_close_position = close_cache.get_short_close_position(close_cache.extreme_small[-extreme_no])
                            break

                    extreme_no += 1
            elif price[pos + 1] < price[pos]:
                if last_trend == ClosePositionFinder.TREND_UP:
                    close_cache.add_extreme_small(pos + 1)
                last_trend = ClosePositionFinder.TREND_DOWN

                short_close_position = close_cache.get_short_close_position(pos + 1)

                # find close position of buy action
                extreme_no = 1
                buy_close_position = len(price) - 1
                while extreme_no <= close_cache.len_small or extreme_no <= close_cache.len_big:
                    if extreme_no <= close_cache.len_small:
                        if price[pos] - price[close_cache.extreme_small[-extreme_no]] >= ClosePositionFinder.CLOSE_THRESHOLD(price[pos]):
                            buy_close_position = close_cache.extreme_small[-extreme_no]
                            while price[pos] - price[buy_close_position - 1] >= ClosePositionFinder.CLOSE_THRESHOLD(price[pos]):
                                buy_close_position -= 1
                            break

                    if extreme_no <= close_cache.len_big:
                        if price[close_cache.extreme_big[-extreme_no]] >= price[pos]:
                            buy_close_position = close_cache.get_buy_close_position(close_cache.extreme_big[-extreme_no])
                            break

                    extreme_no += 1
            else:
                buy_close_position = close_cache.get_buy_close_position(pos + 1)
                short_close_position = close_cache.get_short_close_position(pos + 1)

            close_cache.add_buy_close_position(pos, buy_close_position)
            close_cache.add_short_close_position(pos, short_close_position)

            pos -= 1

        buy_close_positions, short_close_positions = [], []
        for i in range(len(price)):
            buy_close_positions.append(close_cache.get_buy_close_position(i))
            short_close_positions.append(close_cache.get_short_close_position(i))

        return buy_close_positions, short_close_positions
