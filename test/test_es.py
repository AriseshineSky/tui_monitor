import asyncio
from services.es_service import EsService  # 你的 ES 封装服务

async def test_es():
    es = EsService()
    breakpoint() 
    data = es.get_hourly_doc_count("amz_products_api_uk_v2")
    print(data)

if __name__ == "__main__":
    asyncio.run(test_es())

