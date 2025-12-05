import hashlib
from config import RING_SIZE

def sha1_int(data: str) -> int:
    h = hashlib.sha1(data.encode("utf-8")).hexdigest()
    # use lower 32 bits
    return int(h, 16) % RING_SIZE

def in_interval(x: int, a: int, b: int, inclusive_right: bool = True) -> bool:
    """
    Check if x in (a, b] on a ring of size RING_SIZE.
    If inclusive_right=False, check (a, b).
    """
    if a < b:
        if inclusive_right:
            return a < x <= b
        else:
            return a < x < b
    elif a > b:
        # wrapped interval
        if inclusive_right:
            return x > a or x <= b
        else:
            return x > a or x < b
    else:
        # a == b means full circle if inclusive_right
        return inclusive_right
