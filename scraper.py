#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Ürün Scraper v3.2"""

import requests
from bs4 import BeautifulSoup
import json
import time
import re
from xml.etree import ElementTree as ET
from supabase import create_client
from datetime import datetime
import hashlib

TEST_LIMIT = 0  # 0 = Tüm ürünleri scrape et

SUPABASE_URL = "https://zmmpuysxnwqngvlafolm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InptbXB1eXN4bndxbmd2bGFmb2xtIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjkwNjA0MTAsImV4cCI6MjA4NDYzNjQxMH0.4Q7k-cDcaGhOurMlofG8lkd4ApPyYexxkMdXxH-lI0k"

supabase = None

SITE_CONFIGS = {
    'technopluskibris.com': {
        'name': 'TECHNOPLUSKIBRIS',
        'sitemap_url': 'https://technopluskibris.com/sitemap.xml',
        'sitemap_type': 'index',
        'product_sitemap_pattern': r'products_\d+\.xml',
        'product_url_pattern': r'/prd-',
        'selectors': {
            'title': ['h1.product-name', 'h1.product-title', '.product-detail h1', 'h1', 'title'],
            'price': ['.product-price span', '.product-price', 'span[class*="price"]', 'div[class*="price"]'],
            'currency': 'TL'
        }
    },
    'durmazz.com': {
        'name': 'DURMAZZ',
        'sitemap_url': 'https://www.durmazz.com/sitemap.xml',
        'sitemap_type': 'direct',
        'product_url_pattern': r'/shop/[\w\-]+-\d+$',
        'exclude_patterns': [r'/cart', r'/wishlist', r'/category', r'/checkout'],
        'selectors': {
            'title': ['h1[itemprop="name"]', '.product-title h1', 'h1.product-name', 'h1', 'title'],
            'price': ['span[itemprop="price"]', '.oe_currency_value', 'span.oe_price', '.product_price span'],
            'currency': 'USD'
        }
    },
    'irismostore.com': {
        'name': 'IRISMOSTORE',
        'sitemap_type': 'multi',
        'sitemap_urls': [
            'https://www.irismostore.com/xml/sitemap_product_1.xml?sr=689361be13f3c',
            'https://www.irismostore.com/xml/sitemap_product_2.xml?sr=689361be28cb1'
        ],
        'product_url_pattern': r'/urun/',
        'selectors': {
            'title': ['h1', '.product-title', 'title'],
            'price': ['.product-price', 'h3', 'span.price'],
            'currency': 'TL'
        }
    }
}

def generate_sku(url, site_name):
    url_part = url.rstrip('/').split('/')[-1]
    site_prefix = site_name[:3].upper()
    url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
    return f"{site_prefix}-{url_part[:30]}-{url_hash}"

def init_supabase():
    global supabase
    try:
        print("\n🔍 Supabase bağlantısı test ediliyor...")
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        result = supabase.table('products').select("count", count='exact').execute()
        count = result.count if hasattr(result, 'count') else 0
        print(f"  ✅ Bağlantı başarılı! {count} ürün mevcut")
        return True
    except Exception as e:
        print(f"  ❌ Hata: {e}")
        return False

def save_product_to_db(product, site_name):
    try:
        if not supabase:
            return False
        
        sku = generate_sku(product['url'], site_name)
        stock_status = 'in_stock' if product['price'] is not None else 'unknown'
        
        data = {
            'sku': sku,
            'name': product['title'],
            'price': product['price'],
            'stock_status': stock_status,
            'url': product['url'],
            'product_name': product['title'],
            'product_url': product['url'],
            'stock_data': {
                'site': site_name,
                'currency': product.get('currency'),
                'last_seen_price': product.get('price'),
                'scraped_at': datetime.now().isoformat()
            },
            'scraped_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        
        supabase.table('products').upsert(data, on_conflict='sku').execute()
        return True
    except Exception as e:
        print(f"      ❌ DB: {str(e)[:50]}")
        return False

def get_sitemap_urls(sitemap_url):
    try:
        response = requests.get(sitemap_url, timeout=15)
        response.raise_for_status()
        urls = []
        root = ET.fromstring(response.content)
        for loc in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}loc'):
            urls.append(loc.text.strip())
        if not urls:
            for loc in root.findall('.//loc'):
                urls.append(loc.text.strip())
        return urls
    except Exception as e:
        print(f"  ✗ Sitemap: {e}")
        return []

def get_product_sitemaps(config):
    sitemap_type = config.get('sitemap_type', 'direct')
    
    if sitemap_type == 'multi':
        return config.get('sitemap_urls', [])
    
    sitemap_url = config.get('sitemap_url')
    if not sitemap_url:
        return []
    
    if sitemap_type == 'direct':
        return [sitemap_url]
    
    print(f"📑 Sitemap index okunuyor...")
    all_sitemaps = get_sitemap_urls(sitemap_url)
    if not all_sitemaps:
        return []
    
    product_sitemap_pattern = config.get('product_sitemap_pattern', '')
    product_sitemaps = []
    for sitemap in all_sitemaps:
        if product_sitemap_pattern and re.search(product_sitemap_pattern, sitemap):
            product_sitemaps.append(sitemap)
    
    print(f"  ✓ {len(product_sitemaps)} sitemap bulundu")
    return product_sitemaps

def filter_product_urls(urls, config):
    product_urls = []
    product_pattern = config.get('product_url_pattern', '')
    exclude_patterns = config.get('exclude_patterns', [])
    
    for url in urls:
        if product_pattern and re.search(product_pattern, url):
            is_excluded = False
            for exclude_pattern in exclude_patterns:
                if re.search(exclude_pattern, url):
                    is_excluded = True
                    break
            if not is_excluded:
                product_urls.append(url)
    
    return product_urls

def extract_price(soup, selectors):
    for selector in selectors:
        try:
            element = soup.select_one(selector)
            if element:
                if element.name == 'meta':
                    price_text = element.get('content', '')
                else:
                    price_text = element.get_text(strip=True)
                
                price_text = price_text.replace(',', '').replace(' ', '').replace('.', '')
                price_match = re.search(r'(\d+)', price_text)
                
                if price_match:
                    price = float(price_match.group(1))
                    if price > 0:
                        return price
        except:
            continue
    return None

def extract_title(soup, selectors):
    for selector in selectors:
        try:
            element = soup.select_one(selector)
            if element:
                title = element.get_text(strip=True)
                if element.name == 'title':
                    title = re.split(r'\s*[|\-]\s*', title)[0]
                if title and len(title) > 3:
                    return title
        except:
            continue
    return "Bilinmiyor"

def scrape_product(url, config, db_enabled=False):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        title = extract_title(soup, config['selectors']['title'])
        price = extract_price(soup, config['selectors']['price'])
        currency = config['selectors']['currency']
        
        product_data = {'title': title, 'price': price, 'currency': currency, 'url': url}
        
        if db_enabled:
            db_success = save_product_to_db(product_data, config['name'])
            db_icon = "💾" if db_success else "⚠️"
        else:
            db_icon = "📝"
        
        price_str = f"{price} {currency}" if price else "Fiyat yok"
        print(f"    {db_icon} {title[:45]}... - {price_str}")
        
        return product_data
    except Exception as e:
        print(f"    ✗ Error: {str(e)[:50]}")
        return None

def scrape_site(config, db_enabled=False):
    print(f"\n{'='*70}")
    print(f"🏪 {config['name']}")
    print(f"{'='*70}")
    
    products = []
    product_sitemaps = get_product_sitemaps(config)
    
    if not product_sitemaps:
        print("✗ Sitemap bulunamadı")
        return products
    
    all_product_urls = []
    for sitemap_url in product_sitemaps:
        print(f"\n📄 {sitemap_url.split('/')[-1]}")
        urls = get_sitemap_urls(sitemap_url)
        if urls:
            product_urls = filter_product_urls(urls, config)
            all_product_urls.extend(product_urls)
            print(f"  ✓ {len(product_urls)} ürün URL'si")
    
    all_product_urls = list(set(all_product_urls))
    
    if TEST_LIMIT > 0:
        print(f"\n⚠️ TEST: İlk {TEST_LIMIT} ürün")
        all_product_urls = all_product_urls[:TEST_LIMIT]
    
    if not all_product_urls:
        print("✗ Ürün bulunamadı")
        return products
    
    print(f"\n📊 {len(all_product_urls)} ürün scrape edilecek")
    print(f"{'─'*70}")
    
    for i, url in enumerate(all_product_urls, 1):
        print(f"  [{i}/{len(all_product_urls)}]", end=" ")
        product = scrape_product(url, config, db_enabled)
        if product:
            products.append(product)
        
        if i % 10 == 0:
            time.sleep(1)
        else:
            time.sleep(0.3)
    
    print(f"{'─'*70}")
    print(f"✅ {len(products)} ürün tamamlandı")
    return products

def main():
    print(f"\n{'='*70}")
    print("🚀 SCRAPER v3.2")
    print(f"{'='*70}")
    
    db_enabled = init_supabase()
    
    if db_enabled:
        print("💾 JSON + Supabase")
    else:
        print("📝 Sadece JSON")
    
    all_products = []
    stats = {}
    
    for domain, config in SITE_CONFIGS.items():
        products = scrape_site(config, db_enabled)
        site_name = config['name']
        stats[site_name] = {
            'total': len(products),
            'with_price': len([p for p in products if p['price'] is not None]),
            'without_price': len([p for p in products if p['price'] is None])
        }
        for product in products:
            product['site'] = site_name
        all_products.extend(products)
    
    with open('products.json', 'w', encoding='utf-8') as f:
        json.dump(all_products, f, ensure_ascii=False, indent=2)
    
    print(f"\n{'='*70}")
    print("📊 ÖZET")
    print(f"{'='*70}")
    print(f"Toplam: {len(all_products)}")
    for site_name, site_stats in stats.items():
        print(f"\n{site_name}: {site_stats['total']}")
        print(f"  Fiyatlı: {site_stats['with_price']}")
        print(f"  Fiyatsız: {site_stats['without_price']}")
    
    print(f"\n✅ products.json kaydedildi")
    if db_enabled:
        print(f"✅ Supabase güncellendi")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    main()
