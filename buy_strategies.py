
def volatility_breakout(prev_high, prev_low, prev_price, current_price, x=0.5):
    prev_range = prev_high - prev_low

    if current_price < (prev_price - x * prev_range):
        return -1  # short
    elif current_price > (prev_price + x * prev_range):
        return 1  # long
    else:
        return 0  # hold

# def buy_systrader79_short_term(envelope, prev_price, envelop_thresh=105):
#     if envelope >= envelop_thresh and


def get_envelope(current_price, moving_average):
    return current_price / moving_average * 100
