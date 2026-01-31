BARS = "▁▂▃▄▅▆▇█"

def sparkline(data):
    if not data or max(data) == 0:
        return "No data"

    mn, mx = min(data), max(data)
    span = mx - mn or 1

    return "".join(BARS[int((v - mn) / span * (len(BARS) - 1))] for v in data)
