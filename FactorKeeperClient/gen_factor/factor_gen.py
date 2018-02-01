from .close_position import ClosePositionFinder
import pandas as pd
import numpy as np


def factor_generator(df, code, date):
    # ask = np.array(df['ask1'])
    # bid = np.array(df['bid1'])
    # ask[ask == 0] = bid[ask == 0]
    # bid[bid == 0] = ask[bid == 0]
    # price = 0.5 * (ask + bid)
    # buy, short = ClosePositionFinder.find_buy_close_position(price)
    #
    # ret_df = pd.DataFrame({"buy_close_position": buy, "short_close_position": short})
    # return ret_df
    print(df.columns)
    ret_df = pd.DataFrame({"buy_close_position": df['last_002466.SZ'], "short_close_position": df['last_002460.SZ']})
    ret_df['addition_col'] = 0
    return ret_df
