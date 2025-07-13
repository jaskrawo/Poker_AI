from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import tomli
import json



def clean_one_file(file_content):
    file, content = file_content
    file = file.split('/')[-1]
    content = json.loads(content)
    output = []
    RANK_MAP = {
    '2': 2, '3': 3, '4': 4, '5': 5, '6': 6, '7': 7, '8': 8, '9': 9,
    'T': 10, 'J': 11, 'Q': 12, 'K': 13, 'A': 14
    }
    SUIT_MAP = {'c': 0, 'd': 1, 'h': 2, 's': 3}
    BIG_BLIND = content['blinds_or_straddles'][1]

    for position, player in enumerate(content['players']):
        # Find hole cards in actions
        hole_card_str = None
        for action in content['actions']:
            if action.startswith(f'd dh p{position+1}'):
                hole_card_str = action.split()[-1]
                break
        if not hole_card_str:
            continue
            
        card1 = hole_card_str[:2]
        card2 = hole_card_str[2:4]

        # Extract card properties
        rank1 = RANK_MAP[card1[0]]
        suit1 = SUIT_MAP[card1[1]]
        rank2 = RANK_MAP[card2[0]]
        suit2 = SUIT_MAP[card2[1]]
        
        # Calculate hand properties
        is_pair = (rank1 == rank2)
        is_suited = (suit1 == suit2)
        high_card = max(rank1, rank2)
        card_gap = abs(rank1 - rank2)

        net_gain_bb = (content['finishing_stacks'][position] - 
                   content['starting_stacks'][position]) / BIG_BLIND

        player_json = {
            "num_players": len(content['players']),
            "player_id": player,
            "position": position,
            "hole_card_1_rank": rank1,
            "hole_card_2_rank": rank2,
            "is_pair": is_pair,
            "is_suited": is_suited,
            "high_card": high_card,
            "card_gap": card_gap,
            "net_gain_bb": net_gain_bb
        }
        """
        for example:
        {
            "num_players": 6,
            "player_id": "Bill", 
            "position": 4, 
            "hole_card_1_rank": 9, 
            "hole_card_2_rank": 8,
            "is_pair": false,
            "is_suited": true,
            "high_card": 9,
            "card_gap": 1,
            "net_gain_bb": 22.5
        }
        """
        output.append((player + '_' + file, player_json))
    return output

def save_file(file_content):
    file_name, content = file_content
    content = json.dumps(content)
    with open(f"processed_json_files/" + file_name, "w") as outfile:
        outfile.write(content)

spark = SparkSession.builder \
    .appName("PokerHandParser") \
    .master("local[*]") \
    .getOrCreate()



rddFromFile = spark.sparkContext.wholeTextFiles("raw_json_files/*.json")
rdd_json_flat_map = rddFromFile.flatMap(clean_one_file)
rdd_save = rdd_json_flat_map.map(save_file)

rdd_save.collect()