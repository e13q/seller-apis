import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров в магазине Ozon.

    Args:
        last_id (str): Последний просмотренный товар.
        client_id (str): Идентификатор клиента Ozon.
        seller_token (str): Ключ API Ozon.

    Returns:
        list: Список товаров из магазина Ozon в формате json.

    Example:
        >>> get_product_list(last_id, client_id, seller_token)
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров магазина Ozon.

    Args:
        client_id (str): Идентификатор клиента Ozon.
        seller_token (str): Ключ API Ozon.

    Returns:
        offer_ids (list): Список артикулов.

    Example:
        >>> get_offer_ids(client_id, seller_token)
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров в базе Ozon.

    Args:
        prices (list): Список цен.
        client_id (str): Идентификатор клиента Ozon.
        seller_token (str): Ключ API Ozon.

    Returns:
        list: Возвращает ответ API Ozon в формате JSON.

    Example:
        >>> update_price([12000, 13000, 24000], client_id, seller_token)
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки в базе Ozon.

    Args:
        stocks (list): Список остатков.
        client_id (str): Идентификатор клиента Ozon.
        seller_token (str): Ключ API Ozon.

    Returns:
        list: Возвращает ответ API Ozon в формате JSON.

    Example:
        >>> update_stocks(stocks, client_id, seller_token)
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать и получить данные по остаткам товаров из файла с сайта casio.

    Returns:
        watch_remnants (dict): Возвращает список данных в формате DataFrame.

    Example:
        >>> download_stock()
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создать список остатков по файлу ostatki.xls и данным с Ozon.

    Args:
        watch_remnants (dict): Cписок данных из файла ostatki.xls.
        offer_ids (list): Список артикулов.

    Returns:
        stocks (list): Список остатков по файлу ostatki.xls и данным с Ozon.

    Example:
        >>> create_stocks(watch_remnants, offer_ids)
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать список сконвертированных из файла цен.

    Args:
        watch_remnants (dict): Cписок данных из файла ostatki.xls.
        offer_ids (list): Список артикулов.

    Returns:
        prices (list): Список цен.

    Example:
        >>> create_prices(watch_remnants, offer_ids)
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Конвертация строки с ценой в строку с ценой без доп. символов.

    Args:
        price (str): Строка для преобразования, содержащая символы, кроме цифр.

    Returns:
        str: Преобразованная строка, содержащая лишь цифры.

    Correct example:
        >>> print(price_conversion("5'990.00 руб."))
        5990

    Incorrect example:
        >>> print(price_conversion(150))
        price_conversion(15)
        ^^^^^^^^^^^^^^^^
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список lst на части по n элементов.

    Args:
        lst (list): Список, который необходимо разделить.
        n (int): Количество элементов.

    Yields:
        list: Список элементов от i до i + n.

    Example:
        >>> lsts = (i for i in divide([1, 2, 3, 4], 2))
        >>> for lst in lsts:
        >>>    print(lst)
        [1, 2]
        [3, 4]

    Incorrect example:
        >>> lsts = (i for i in divide([1, 2, 3, 4], 0))
        >>> for lst in lsts:
        >>>    print(lst)
        divide([1, 2, 3, 4], 0)
        ^^^^^^^^^^^^^^^^^^^^^^^
    """
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Загрузить цены.

    Args:
        watch_remnants (dict): Cписок данных из файла ostatki.xls.
        client_id (str): Идентификатор клиента Ozon.
        seller_token (str): Ключ API Ozon.

    Returns:
        list: Список обновлённых цен.

    Correct example:
        >>> upload_prices(watch_remnants, client_id, seller_token)
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Загрузить остатки.

    Args:
        watch_remnants (dict): Cписок данных из файла ostatki.xls.
        client_id (str): Идентификатор клиента Ozon.
        seller_token (str): Ключ API Ozon.

    Returns:
        tuple: Список не пустых остатков и список остатков.

    Correct example:
        >>> upload_stocks(watch_remnants, client_id, seller_token)
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """Основная функция, обновляющая остатки и изменяющая цены.

    Raises:
        ReadTimeout: Превышено время ожидания.
        ConnectionError: Ошибка соединения.
        Exception: ERROR_2.

    Correct example:
        >>> main()
    """
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
