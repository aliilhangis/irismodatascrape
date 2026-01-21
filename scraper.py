#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
İyileştirilmiş Ürün Scraper
- Sitemap'ten direkt ürün linklerini alır
- Her site için özelleştirilmiş selector'lar
- Gelişmiş fiyat ve başlık çıkarma
"""

import requests
from bs4 import BeautifulSoup
import json
from urllib.parse import urljoin, urlparse
import time
import re
from xml.etree import ElementTree as ET

# Site konfigürasyonları - Her site için özel ayarlar
SITE_CONFIGS = {
    'technopluskibris.com': {
        'name': 'TECHNOPLUSKIBRIS',
        'sitemap_url': 'https://technopluskibris.com/sitemap.xml',
        'product_url_pattern': r'/prd-',  # Ürün URL pattern'i
        'selectors': {
            'title': [
                'h1.product-name',
                'h1.product-title', 
                '.product-detail h1',
                'h1'
            ],
            'price': [
                '.product-price .price',
                '.price-tag',
                'span[itemprop="price"]',
                '.product-price',
                'meta[property="product:price:amount"]'
            ],
            'currency': 'TL'
        }
    },
    'durmazz.com': {
        'name': 'DURMAZZ',
        'sitemap_url': 'https://www.durmazz.com/sitemap.xml',
        'product_url_pattern': r'/shop/product/',  # Ürün URL pattern'i
        'exclude_patterns': [r'/cart', r'/wishlist', r'/category', r'/checkout'],
        'selectors': {
            'title': [
                'h1.product-name',
                '.product-title h1',
                'h1[itemprop="name"]',
                '.product-detail h1',
                'h1'
            ],
            'price': [
                '.product-price',
                'span[itemprop="price"]',
                '.price-tag',
                'meta[property="product:price:amount"]',
                '.price'
            ],
            'currency': 'USD'
        }
    }
}

def get_sitemap_urls(sitemap_url):
    """Sitemap'ten tüm URL'leri çeker"""
    try:
        print(f"Sitemap alınıyor: {sitemap_url}")
        response = requests.get(sitemap_url, timeout=10)
        response.raise_for_status()
        
        urls = []
        
        # XML parse et
        root = ET.fromstring(response.content)
        
        # XML namespace'i bul
        namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        
        # <loc> taglerini bul
        for loc in root.findall('.//ns:loc', namespace):
            url = loc.text.strip()
            urls.append(url)
        
        # Eğer namespace ile bulamazsa, namespace olmadan dene
        if not urls:
            for loc in root.findall('.//loc'):
                url = loc.text.strip()
                urls.append(url)
        
        print(f"✓ Sitemap'ten {len(urls)} URL bulundu")
        return urls
        
    except Exception as e:
        print(f"✗ Sitemap hatası: {e}")
        return []

def filter_product_urls(urls, config):
    """URL'leri filtrele - sadece ürün sayfalarını al"""
    product_urls = []
    
    product_pattern = config.get('product_url_pattern', '')
    exclude_patterns = config.get('exclude_patterns', [])
    
    for url in urls:
        # Ürün pattern'ini kontrol et
        if product_pattern and re.search(product_pattern, url):
            # Hariç tutulacak pattern'leri kontrol et
            is_excluded = False
            for exclude_pattern in exclude_patterns:
                if re.search(exclude_pattern, url):
                    is_excluded = True
                    break
            
            if not is_excluded:
                product_urls.append(url)
    
    return product_urls

def extract_price(soup, selectors):
    """Sayfadan fiyatı çıkar - birden fazla selector dene"""
    for selector in selectors:
        try:
            # CSS selector
            element = soup.select_one(selector)
            if element:
                # Meta tag ise content attribute'unu al
                if element.name == 'meta':
                    price_text = element.get('content', '')
                else:
                    price_text = element.get_text(strip=True)
                
                # Fiyat rakamlarını çıkar
                price_match = re.search(r'[\d.,]+', price_text.replace(',', ''))
                if price_match:
                    try:
                        price = float(price_match.group().replace(',', '.'))
                        return price
                    except:
                        continue
        except:
            continue
    
    return None

def extract_title(soup, selectors):
    """Sayfadan başlığı çıkar - birden fazla selector dene"""
    for selector in selectors:
        try:
            element = soup.select_one(selector)
            if element:
                title = element.get_text(strip=True)
                if title and len(title) > 3:  # Çok kısa başlıkları reddet
                    return title
        except:
            continue
    
    return "Bilinmiyor"

def scrape_product(url, config):
    """Tek bir ürün sayfasını scrape et"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Başlık ve fiyat çıkar
        title = extract_title(soup, config['selectors']['title'])
        price = extract_price(soup, config['selectors']['price'])
        currency = config['selectors']['currency']
        
        product_data = {
            'title': title,
            'price': price,
            'currency': currency,
            'url': url
        }
        
        # Konsola yazdır
        price_display = f"{price} {currency}" if price is not None else "Fiyat bulunamadı"
        print(f"  ✓ {title[:50]}... - {price_display}")
        
        return product_data
        
    except Exception as e:
        print(f"  ✗ Hata ({url}): {e}")
        return None

def scrape_site(config):
    """Bir siteyi tamamen scrape et"""
    print(f"\n{'='*60}")
    print(f"SİTE: {config['name']}")
    print(f"{'='*60}")
    
    products = []
    
    # Sitemap'ten URL'leri al
    all_urls = get_sitemap_urls(config['sitemap_url'])
    
    if not all_urls:
        print("✗ Sitemap'ten URL alınamadı")
        return products
    
    # Ürün URL'lerini filtrele
    product_urls = filter_product_urls(all_urls, config)
    
    print(f"\n✓ {len(product_urls)} ürün URL'si bulundu")
    print(f"İlk birkaç ürün URL'si:")
    for url in product_urls[:3]:
        print(f"  - {url}")
    
    # Her ürünü scrape et
    print(f"\nÜrünler scrape ediliyor...")
    for i, url in enumerate(product_urls, 1):
        print(f"\n[{i}/{len(product_urls)}] Scraping: {url}")
        
        product = scrape_product(url, config)
        
        if product:
            products.append(product)
        
        # Rate limiting - siteyе çok yük bindirmemek için
        time.sleep(0.5)
    
    print(f"\n✓ Toplam {len(products)} ürün başarıyla scrape edildi")
    
    return products

def main():
    """Ana fonksiyon"""
    all_products = []
    stats = {}
    
    # Her siteyi scrape et
    for domain, config in SITE_CONFIGS.items():
        products = scrape_site(config)
        
        # İstatistikleri kaydet
        site_name = config['name'].lower().replace(' ', '_')
        stats[site_name] = len(products)
        
        # Site bilgisini her ürüne ekle
        for product in products:
            product['site'] = config['name']
        
        all_products.extend(products)
        
        print(f"\n{config['name']}: {len(products)} ürün")
    
    # JSON'a kaydet
    output_file = 'products.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_products, f, ensure_ascii=False, indent=2)
    
    print(f"\n{'='*60}")
    print(f"✓ {len(all_products)} ürün {output_file} dosyasına kaydedildi")
    print(f"{'='*60}")
    
    # Özet istatistikler
    print(f"\n{'='*60}")
    print("ÖZET İSTATİSTİKLER")
    print(f"{'='*60}")
    print(f"Toplam Ürün: {len(all_products)}")
    for site_name, count in stats.items():
        print(f"{site_name.replace('_', ' ').title()}: {count}")
    
    # Fiyat istatistikleri
    products_with_price = [p for p in all_products if p['price'] is not None]
    products_without_price = [p for p in all_products if p['price'] is None]
    
    print(f"\nFiyatı olan ürünler: {len(products_with_price)}")
    print(f"Fiyatı olmayan ürünler: {len(products_without_price)}")
    
    if products_without_price:
        print(f"\nFiyatı bulunamayan ürünler:")
        for p in products_without_price[:5]:  # İlk 5'ini göster
            print(f"  - {p['title'][:60]} ({p['site']})")

if __name__ == "__main__":
    main()
