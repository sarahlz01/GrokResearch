import json
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

sns.set(style="whitegrid")

# fucntion to load JSON data from file 

def load_json_data(filepath: str):
    with open(filepath,"r", encoding="utf-8") as f:
        return json.load(f)
