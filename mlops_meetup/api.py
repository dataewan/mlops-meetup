from typing import Dict, List
from fastapi import HTTPException


def check_user_in_index(user_id: str, user_index) -> bool:
    if user_index is None:
        raise HTTPException(status_code=500, detail="user index not ready")

    if not user_id in user_index:
        raise HTTPException(status_code=404, detail="user not found in index")


def get_user_metadata(user_id, user_index):
    return user_index[user_id]


def get_neighbour_ids(user_metadata, nn_index):
    seed = user_metadata.seed_embedding
    return nn_index.get_nns_by_vector(seed, 20)


def item_ids_to_items(
    items: List[int], reverse_item_lookup: Dict[int, str]
) -> List[str]:
    filtered = [reverse_item_lookup[i] for i in items if i in reverse_item_lookup]
    return filtered


def filter_neighbours(neighbours: List[str], user_metadata) -> List[str]:
    previously_reviewed = user_metadata.items
    return [n for n in neighbours if not n in previously_reviewed]


def get_user_recs(user_id: str, nn_index, user_index, reverse_item_lookup):
    check_user_in_index(user_id, user_index)

    user_metadata = get_user_metadata(user_id, user_index)
    neighbour_ids = get_neighbour_ids(user_metadata, nn_index)
    neighbours = item_ids_to_items(neighbour_ids, reverse_item_lookup)

    neighbours_w_business_rules = filter_neighbours(neighbours, user_metadata)
    return neighbours_w_business_rules
