#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
İyileştirilmiş Ürün Scraper v2.0
- Sitemap index support
- Her site için özel pattern'ler  
- Gelişmiş fiyat çıkarma
"""

import requests
from bs4 import BeautifulSoup
import json
from urllib.parse import urljoin
import time
import re
from xml.etree import ElementTree as ET

# Site konfigürasyonları
SITE_CONFIGS = {
    'technopluskibris.com': {
        'name': 'TECHNOPLUSKIBRIS',
        'sitemap_url': 'https://technopluskibris.com/sitemap.xml',
        'sitemap_type': 'index',  # Sitemap index
        'product_sitemap_pattern': r'products_\d+\.xml',  # Ürün sitemap pattern'i
        'product_url_pattern': r'/prd-',
        'selectors': {
            'title': [
                'h1.product-name',
                'h1.product-title',
                '.product-detail h1',
                'h1',
                'title'
            ],
            'price': [
                '.product-price span',
                '.product-price',
                'span[class*="price"]',
                'div[class*="price"]',
                'meta[property="product:price:amount"]'
            ],
            'currency': 'TL'
        }
    },
    'durmazz.com': {
        'name': 'DURMAZZ',
        'sitemap_url': 'https://www.durmazz.com/sitemap.xml',
        'sitemap_type': 'index',
        'product_sitemap_pattern': r'shop-sitemap\.xml',
        'product_url_pattern': r'/shop/(product-\d+|[a-z0-9\-]+\-\d+)',
        'exclude_patterns': [r'/cart', r'/wishlist', r'/category', r'/checkout', r'/page/', r'/compare'],
        'selectors': {
            'title': [
                'h1[itemprop="name"]',
                '.product-title h1',
                'h1.product-name',
                '.oe_product h1',
                'h1'
            ],
            'price': [
                'span[itemprop="price"]',
                '.oe_currency_value',
                'span.oe_price',
                'div[class*="price"] span',
                'meta[property="product:price:amount"]'
            ],
            'currency': 'USD'
        }
    }
}

def get_sitemap_urls(sitemap_url):
    """Sitemap'ten URL'leri çeker"""
    try:
        print(f"  Fetching: {sitemap_url}")
        response = requests.get(sitemap_url, timeout=15)
        response.raise_for_status()
        
        urls = []
        root = ET.fromstring(response.content)
        
        # Namespace
        namespaces = {
            'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9',
            '': 'http://www.sitemaps.org/schemas/sitemap/0.9'
        }
        
        # <loc> taglerini bul
        for loc in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc'):
            urls.append(loc.text.strip())
        
        # Namespace olmadan da dene
        if not urls:
            for loc in root.findall('.//loc'):
                urls.append(loc.text.strip())
        
        return urls
        
    except Exception as e:
        print(f"  ✗ Sitemap error: {e}")
        return []

def get_product_sitemaps(config):
    """Sitemap index'ten ürün sitemap'lerini bulur"""
    sitemap_url = config['sitemap_url']
    sitemap_type = config.get('sitemap_type', 'direct')
    
    if sitemap_type == 'direct':
        # Direkt ürün sitemap'i
        return [sitemap_url]
    
    # Sitemap index - alt sitemap'leri bul
    print(f"📑 Sitemap index okunuyor...")
    all_sitemaps = get_sitemap_urls(sitemap_url)
    
    if not all_sitemaps:
        return []
    
    # Ürün sitemap'lerini filtrele
    product_sitemap_pattern = config.get('product_sitemap_pattern', '')
    product_sitemaps = []
    
    for sitemap in all_sitemaps:
        if product_sitemap_pattern and re.search(product_sitemap_pattern, sitemap):
            product_sitemaps.append(sitemap)
    
    print(f"  ✓ {len(product_sitemaps)} ürün sitemap bulundu")
    return product_sitemaps

def filter_product_urls(urls, config):
    """URL'leri filtrele"""
    product_urls = []
    product_pattern = config.get('product_url_pattern', '')
    exclude_patterns = config.get('exclude_patterns', [])
    
    for url in urls:
        # Ürün pattern kontrolü
        if product_pattern and re.search(product_pattern, url):
            # Exclude kontrolü
            is_excluded = False
            for exclude_pattern in exclude_patterns:
                if re.search(exclude_pattern, url):
                    is_excluded = True
                    break
            
            if not is_excluded:
                product_urls.append(url)
    
    return product_urls

def extract_price(soup, selectors):
    """Fiyat çıkar"""
    for selector in selectors:
        try:
            element = soup.select_one(selector)
            if element:
                # Meta tag
                if element.name == 'meta':
                    price_text = element.get('content', '')
                else:
                    price_text = element.get_text(strip=True)
                
                # Fiyat parse
                price_text = price_text.replace(',', '').replace(' ', '')
                price_match = re.search(r'(\d+\.?\d*)', price_text)
                
                if price_match:
                    try:
                        price = float(price_match.group(1))
                        if price > 0:  # Sıfır fiyatları reddet
                            return price
                    except:
                        continue
        except:
            continue
    
    return None

def extract_title(soup, selectors):
    """Başlık çıkar"""
    for selector in selectors:
        try:
            element = soup.select_one(selector)
            if element:
                title = element.get_text(strip=True)
                # Title tag ise, meta bilgilerini temizle
                if element.name == 'title':
                    # " | Site Adı" gibi kısımları kaldır
                    title = re.split(r'\s*[|\-]\s*', title)[0]
                
                if title and len(title) > 3:
                    return title
        except:
            continue
    
    return "Bilinmiyor"

def scrape_product(url, config):
    """Ürün scrape et"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Başlık ve fiyat
        title = extract_title(soup, config['selectors']['title'])
        price = extract_price(soup, config['selectors']['price'])
        currency = config['selectors']['currency']
        
        product_data = {
            'title': title,
            'price': price,
            'currency': currency,
            'url': url
        }
        
        # Log
        if price is not None:
            print(f"    ✓ {title[:50]}... - {price} {currency}")
        else:
            print(f"    ⚠ {title[:50]}... - Fiyat bulunamadı")
        
        return product_data
        
    except Exception as e:
        print(f"    ✗ Error: {str(e)[:50]}")
        return None

def scrape_site(config):
    """Site scrape et"""
    print(f"\n{'='*70}")
    print(f"🏪 SİTE: {config['name']}")
    print(f"{'='*70}")
    
    products = []
    
    # Ürün sitemap'lerini al
    product_sitemaps = get_product_sitemaps(config)
    
    if not product_sitemaps:
        print("✗ Ürün sitemap bulunamadı")
        return products
    
    # Her sitemap'ten URL'leri al
    all_product_urls = []
    for sitemap_url in product_sitemaps:
        print(f"\n📄 Sitemap: {sitemap_url.split('/')[-1]}")
        urls = get_sitemap_urls(sitemap_url)
        
        if urls:
            # Ürün URL'lerini filtrele
            product_urls = filter_product_urls(urls, config)
            all_product_urls.extend(product_urls)
            print(f"  ✓ {len(product_urls)} ürün URL'si bulundu")
    
    # Duplicate'leri kaldır
    all_product_urls = list(set(all_product_urls))
    
    if not all_product_urls:
        print("\n✗ Hiç ürün URL'si bulunamadı")
        return products
    
    print(f"\n📊 Toplam: {len(all_product_urls)} benzersiz ürün URL'si")
    print(f"\n🔍 İlk 3 URL örneği:")
    for url in all_product_urls[:3]:
        print(f"  • {url}")
    
    # Scrape et
    print(f"\n⚙️ Ürünler scrape ediliyor...")
    print(f"{'─'*70}")
    
    for i, url in enumerate(all_product_urls, 1):
        print(f"  [{i}/{len(all_product_urls)}]", end=" ")
        
        product = scrape_product(url, config)
        
        if product:
            products.append(product)
        
        # Rate limiting
        if i % 10 == 0:
            time.sleep(1)
        else:
            time.sleep(0.3)
    
    print(f"{'─'*70}")
    print(f"✅ {len(products)} ürün başarıyla scrape edildi")
    
    return products

def main():
    """Ana fonksiyon"""
    print(f"\n{'='*70}")
    print("🚀 ÜRÜN SCRAPER BAŞLATILIYOR")
    print(f"{'='*70}")
    
    all_products = []
    stats = {}
    
    # Her site
    for domain, config in SITE_CONFIGS.items():
        products = scrape_site(config)
        
        # Stats
        site_name = config['name']
        stats[site_name] = {
            'total': len(products),
            'with_price': len([p for p in products if p['price'] is not None]),
            'without_price': len([p for p in products if p['price'] is None])
        }
        
        # Site bilgisi ekle
        for product in products:
            product['site'] = site_name
        
        all_products.extend(products)
    
    # JSON'a kaydet
    output_file = 'products.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_products, f, ensure_ascii=False, indent=2)
    
    # Özet
    print(f"\n{'='*70}")
    print("📊 ÖZET İSTATİSTİKLER")
    print(f"{'='*70}")
    print(f"Toplam Ürün: {len(all_products)}")
    
    for site_name, site_stats in stats.items():
        print(f"\n{site_name}:")
        print(f"  • Toplam: {site_stats['total']}")
        print(f"  • Fiyatlı: {site_stats['with_price']}")
        print(f"  • Fiyatsız: {site_stats['without_price']}")
    
    print(f"\n✅ Veriler '{output_file}' dosyasına kaydedildi")
    print(f"{'='*70}\n")
    
    # Fiyatsız ürünler
    products_without_price = [p for p in all_products if p['price'] is None]
    if products_without_price:
        print(f"\n⚠️ Fiyatı bulunamayan ürünler ({len(products_without_price)}):")
        for p in products_without_price[:10]:
            print(f"  • {p['title'][:60]} ({p['site']})")
        if len(products_without_price) > 10:
            print(f"  ... ve {len(products_without_price) - 10} ürün daha")

if __name__ == "__main__":
    main()
