from annoy import AnnoyIndex
import prefect
from typing import Tuple, Dict, List
import itertools
from dataclasses import dataclass
import numpy as np
import pickle
from mlops_meetup import config


@dataclass
class UserMetadata:
    items: List[str]
    seed_embedding: np.ndarray


@prefect.task
def extract_item_embeddings(model):
    item_embeddings = model.get_layer("item_embedding").embeddings
    return item_embeddings


@prefect.task
def make_nn_index(item_embeddings, reverse_item_lookup):
    index = AnnoyIndex(config.EMBEDDING_SIZE, "dot")
    for key in reverse_item_lookup.keys():
        index.add_item(key, item_embeddings[key, :].numpy())

    index.build(10)
    return index


@prefect.task
def persist_nn_index(index, path: str):
    index.save(path)


def load_nn_index(path: str):
    index = AnnoyIndex(config.EMBEDDING_SIZE, "dot")
    index.load(path)
    return index


def group_users(user_metadata: List[Tuple[str, str]]) -> List[Tuple[str, List[str]]]:
    grouped = []
    for user, group in itertools.groupby(user_metadata, lambda x: x[0]):
        grouped.append((user, [i[1] for i in group]))

    return grouped


def get_seed(user_items, item_lookup, item_embeddings) -> np.ndarray:
    if user_items[-1] in item_lookup:
        seed_index = item_lookup[user_items[-1]]
        return item_embeddings[seed_index, :].numpy()
    else:
        return item_embeddings.numpy().mean(axis=0)


def make_user_metadata(user, item_lookup, item_embeddings) -> UserMetadata:
    user_id, user_items = user
    seed_embedding = get_seed(user_items, item_lookup, item_embeddings)
    return UserMetadata(items=user_items, seed_embedding=seed_embedding)


@prefect.task
def make_user_index(
    user_metadata, item_lookup, item_embeddings
) -> Dict[str, UserMetadata]:
    grouped_users = group_users(user_metadata)
    user_index = {}
    for user in grouped_users:
        user_index[user[0]] = make_user_metadata(user, item_lookup, item_embeddings)

    return user_index


@prefect.task
def flip_reverse_lookup(reverse_item_lookup: Dict[int, str]) -> Dict[str, int]:
    return {v: k for k, v in reverse_item_lookup.items()}


@prefect.task
def persist_user_index(index, path):
    pickle.dump(index, open(path, "wb"))


def load_user_index(path):
    return pickle.load(open(path, "rb"))
