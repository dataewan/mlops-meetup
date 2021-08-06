from annoy import AnnoyIndex
import prefect


@prefect.task
def extract_item_embeddings(model):
    item_embeddings = model.get_layer("item_embedding").embeddings
    return item_embeddings


@prefect.task
def make_nn_index(item_embeddings, reverse_item_lookup):
    dim = item_embeddings[0, :].shape[0]
    index = AnnoyIndex(dim, "dot")
    for key in reverse_item_lookup.keys():
        index.add_item(key, item_embeddings[key, :])

    index.build(10)
    return index


@prefect.task
def persist_nn_index(index, path: str):
    index.save(path)
