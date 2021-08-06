import prefect
import numpy as np
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers
from typing import Dict, Tuple
from mlops_meetup import config


def create_model(max_item_id: int, max_user_id: int):
    user_input = layers.Input(shape=(1,), name="user")
    item_input = layers.Input(shape=(1,), name="item")

    user_embedding = layers.Embedding(
        name="user_embedding",
        input_dim=max_user_id + 1,
        output_dim=config.EMBEDDING_SIZE,
    )(user_input)
    item_embedding = layers.Embedding(
        name="item_embedding",
        input_dim=max_item_id + 1,
        output_dim=config.EMBEDDING_SIZE,
    )(item_input)

    dot = layers.Dot(
        name="dot_product",
        normalize=True,
        axes=2,
    )([item_embedding, user_embedding])

    merged = layers.Reshape((1,))(dot)

    model = keras.Model(inputs=[user_input, item_input], outputs=[merged])

    model.compile(optimizer="nadam", loss="mse")
    return model


def make_empty_batch(n_positive_samples: int, negative_ratio: int) -> np.ndarray:
    batch_size = (1 + negative_ratio) * n_positive_samples
    batch = np.zeros((batch_size, 3))
    return batch


def negative_sampler(
    data: Dict[str, np.ndarray], positive_indexes, negative_ratio: int
) -> Tuple[np.ndarray, np.ndarray]:
    mask = np.ones(len(data["user_id"]), dtype=np.bool)
    mask[positive_indexes] = 0
    users = data["user_id"][positive_indexes]
    negative_users = np.tile(users, negative_ratio)

    items = data["item_id"][mask]
    negative_items = np.random.choice(items, len(negative_users))

    return (negative_users, negative_items)


def make_batchifier(data, n_positive_samples, negative_ratio):
    batch = make_empty_batch(n_positive_samples, negative_ratio)
    ids = np.arange(len(data["user_id"]))
    while True:
        positive_indexes = np.random.choice(ids, n_positive_samples)
        batch[:n_positive_samples, 0] = data["user_id"][positive_indexes]
        batch[:n_positive_samples, 1] = data["item_id"][positive_indexes]
        batch[:n_positive_samples, 2] = 1

        # make negative samples -- things that they haven't reviewed
        negative_users, negative_items = negative_sampler(
            data, positive_indexes, negative_ratio
        )
        batch[n_positive_samples:, 0] = negative_users
        batch[n_positive_samples:, 1] = negative_items
        batch[n_positive_samples:, 2] = -1

        np.random.shuffle(batch)

        yield {
            "user": batch[:, 0],
            "item": batch[:, 1],
        }, batch[:, 2]


@prefect.task
def train_model(
    training_data: Dict[str, np.ndarray], max_item_id: int, max_user_id: int
):
    n_positive_samples = 128
    model = create_model(max_item_id, max_user_id)
    batchifier = make_batchifier(
        training_data, n_positive_samples=n_positive_samples, negative_ratio=5
    )

    tensorboard_callback = keras.callbacks.TensorBoard(
        log_dir=config.LOGGING_PATH, histogram_freq=1
    )

    model.fit(
        batchifier,
        epochs=25,
        steps_per_epoch=len(training_data["user_id"]) // n_positive_samples,
        verbose=2,
        callbacks=[tensorboard_callback],
    )

    return model


@prefect.task
def save_model(model, model_path: str):
    keras.models.save_model(model, model_path)


@prefect.task
def load_model(model_path: str):
    return keras.models.load_model(model_path)
