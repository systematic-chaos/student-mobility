"""pandas - util/functions.py

Student Mobility

Javier Fernández-Bravo Peñuela
Universitat Politècnica de València
"""

import math

def round_half_down(n, decimals=3):
    multiplier = 10 ** decimals
    return math.ceil(n * multiplier - 0.5) / multiplier
