import asyncio
import aiohttp


async def fetch_page(session, url, headers):
    async with session.get(url, headers=headers) as response:
        return response.status, await response.text()


async def process_links(links, headers):
    async with aiohttp.ClientSession() as session:
        tasks = []
        for link in links:
            task = asyncio.create_task(fetch_page(session, link, headers))
            tasks.append(task)

        responses = await asyncio.gather(*tasks)
        return responses


async def main(links_list,header, chunk_size=3):
    # Split links into chunks for concurrency
    link_chunks = [links_list[i:i+chunk_size] for i in range(0, len(links_list), chunk_size)]
    
    skuId = []
    # Process links asynchronously
    for chunk in link_chunks:
        responses = await process_links(chunk, header)
        for response in responses:
            try:
                soup = BeautifulSoup(response[1], 'html.parser')
                sku_produtos = soup.findAll('input', class_='product-sku')
                sku_pagina = [produto['value'] for produto in sku_produtos]
                skuId.extend(sku_pagina)
            except (AttributeError, Exception) as e:
                print('Error:', e)
    return skuId


if __name__ == "__main__":
    print('Começando o processo de extração')
    asyncio.run(main())