import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получить список товаров в магазине Yandex Market.

    Args:
        page (str): Последний просмотренный товар.
        campaign_id (str): Идентификатор компании на Yandex Market.
        access_token (str): Ключ API Yandex Market.

    Returns:
        any: Данные из ответа поля "result" ответа в формате json.

    Example:
        >>> get_product_list(page, campaign_id, access_token)
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновить остатки в базе Yandex Market.

    Args:
        stocks (list): Список остатков.
        campaign_id (str): Идентификатор компании на Yandex Market.
        access_token (str): Ключ API Yandex Market.

    Returns:
        any: Возвращает ответ API Yandex Market в формате JSON.

    Example:
        >>> update_stocks(stocks, campaign_id, access_token)
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновить цены товаров в базе Yandex Market.

    Args:
        prices (list): Список цен.
        campaign_id (str): Идентификатор компании на Yandex Market.
        access_token (str): Ключ API Yandex Market.

    Returns:
        any: Возвращает ответ API Yandex Market в формате JSON.

    Example:
        >>> update_price([12000, 13000, 24000], campaign_id, access_token)
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить артикулы товаров магазина Yandex Market.

    Args:
        campaign_id (str): Идентификатор компании на Yandex Market.
        market_token (str): Ключ API Yandex Market.

    Returns:
        offer_ids (list): Список артикулов.

    Example:
        >>> get_offer_ids(campaign_id, market_token)
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Создать список остатков по файлу ostatki.xls и данным с Yandex Market.

    Args:
        watch_remnants (dict): Cписок данных из файла ostatki.xls.
        offer_ids (list): Список артикулов.
        warehouse_id (str): Идентификатор склада.

    Returns:
        stocks (list): Список остатков по файлу ostatki.xls и данным с Yandex Market.

    Example:
        >>> create_stocks(watch_remnants, offer_ids, warehouse_id)
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
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
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Загрузить цены.

    Args:
        watch_remnants (dict): Cписок данных из файла ostatki.xls.
        campaign_id (str): Идентификатор компании на Yandex Market.
        market_token (str): Ключ API Yandex Market.

    Returns:
        list: Список обновлённых цен.

    Correct example:
        >>> upload_prices(watch_remnants, campaign_id, market_token)
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Загрузить остатки.

    Args:
        watch_remnants (dict): Cписок данных из файла ostatki.xls.
        campaign_id (str): Идентификатор компании на Yandex Market.
        market_token (str): Ключ API Yandex Market.
        warehouse_id (str): Идентификатор склада.

    Returns:
        tuple: Список не пустых остатков и список остатков.

    Correct example:
        >>> upload_stocks(watch_remnants,  campaign_id, market_token, warehouse_id)
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    """Основная функция, обновляющая остатки и изменяющая цены.

    Raises:
        ReadTimeout: Превышено время ожидания.
        ConnectionError: Ошибка соединения.
        Exception: ERROR_2.

    Notes:
        DBS - Delivery by Seller
        (доставкой, хранением и сборкой занимается владелец).
        FBS - Fulfillment by Seller
        (доставкой занимается Yandex, лишь хранение и сборка на владельце).

    Correct example:
        >>> main()
    """
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
