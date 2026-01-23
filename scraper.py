#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
İyileştirilmiş Ürün Scraper v3.0 - Supabase SDK
- Sitemap index support
- Supabase REST API (Railway compatible)
- Her site için özel pattern'ler  
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import re
from xml.etree import ElementTree as ET
from supabase import create_client
from datetime import datetime
import hashlib

# TEST MODE - Sadece ilk N ürünü scrape et (0 = tümü)
TEST_LIMIT = 10  # Test için 10 ürün, production'da 0 yapın

# Supabase Configuration
SUPABASE_URL = "https://zmmpuysxnwqngvlafolm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InptbXB1eXN4bndxbmd2bGFmb2xtIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Mzc1Mzg0ODksImV4cCI6MjA1MzExNDQ4OX0.wJFipYdPBkxr-vxT4yD-0_AvMXAQhjC83OQGVoqo2Q4"

# Supabase client
supabase = None

# Site konfigürasyonları
SITE_CONFIGS = {
    'technopluskibris.com': {
        'name': 'TECHNOPLUSKIBRIS',
        'sitemap_url': 'https://technopluskibris.com/sitemap.xml',
        'sitemap_type': 'index',
        'product_sitemap_pattern': r'products_\d+\.xml',
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
        'sitemap_type': 'direct',  # direct olarak değiştirdik
        'product_url_pattern': r'/shop/[a-z0-9\-]+-\d+
}

def generate_sku(url, site_name):
    """URL'den benzersiz SKU oluştur"""
    url_part = url.rstrip('/').split('/')[-1]
    site_prefix = site_name[:3].upper()
    url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
    return f"{site_prefix}-{url_part[:30]}-{url_hash}"

def init_supabase():
    """Supabase client'ı başlat"""
    global supabase
    try:
        print("\n🔍 Supabase bağlantısı test ediliyor...")
        print(f"  🔌 REST API ile bağlanılıyor...")
        
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # Test sorgusu
        result = supabase.table('products').select("count", count='exact').execute()
        count = result.count if hasattr(result, 'count') else 0
        
        print(f"  ✅ Bağlantı başarılı!")
        print(f"✅ Veritabanında şu anda {count} ürün var")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Bağlantı hatası: {e}")
        print(f"  ℹ️ Supabase anon key doğru mu kontrol edin")
        return False

def save_product_to_db(product, site_name):
    """Ürünü Supabase'e kaydet (REST API)"""
    try:
        if not supabase:
            return False
        
        sku = generate_sku(product['url'], site_name)
        stock_status = 'in_stock' if product['price'] is not None else 'unknown'
        
        stock_data = {
            'site': site_name,
            'currency': product.get('currency'),
            'last_seen_price': product.get('price'),
            'scraped_at': datetime.now().isoformat()
        }
        
        now = datetime.now().isoformat()
        
        # Upsert (insert or update)
        data = {
            'sku': sku,
            'name': product['title'],
            'price': product['price'],
            'stock_status': stock_status,
            'url': product['url'],
            'product_name': product['title'],
            'product_url': product['url'],
            'stock_data': stock_data,
            'scraped_at': now,
            'updated_at': now
        }
        
        # Upsert: conflict on sku
        supabase.table('products').upsert(data, on_conflict='sku').execute()
        
        return True
        
    except Exception as e:
        print(f"      ❌ DB kayıt hatası: {str(e)[:80]}")
        return False

def get_sitemap_urls(sitemap_url):
    """Sitemap'ten URL'leri çeker"""
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
        print(f"  ✗ Sitemap error: {e}")
        return []

def get_product_sitemaps(config):
    """Sitemap index'ten ürün sitemap'lerini bulur"""
    sitemap_url = config['sitemap_url']
    sitemap_type = config.get('sitemap_type', 'direct')
    
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
    
    print(f"  ✓ {len(product_sitemaps)} ürün sitemap bulundu")
    return product_sitemaps

def filter_product_urls(urls, config):
    """URL'leri filtrele"""
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
    """Fiyat çıkar"""
    for selector in selectors:
        try:
            element = soup.select_one(selector)
            if element:
                if element.name == 'meta':
                    price_text = element.get('content', '')
                else:
                    price_text = element.get_text(strip=True)
                
                price_text = price_text.replace(',', '').replace(' ', '')
                price_match = re.search(r'(\d+\.?\d*)', price_text)
                
                if price_match:
                    try:
                        price = float(price_match.group(1))
                        if price > 0:
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
                if element.name == 'title':
                    title = re.split(r'\s*[|\-]\s*', title)[0]
                
                if title and len(title) > 3:
                    return title
        except:
            continue
    
    return "Bilinmiyor"

def scrape_product(url, config, db_enabled=False):
    """Ürün scrape et"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        title = extract_title(soup, config['selectors']['title'])
        price = extract_price(soup, config['selectors']['price'])
        currency = config['selectors']['currency']
        
        product_data = {
            'title': title,
            'price': price,
            'currency': currency,
            'url': url
        }
        
        if db_enabled:
            db_success = save_product_to_db(product_data, config['name'])
            db_icon = "💾" if db_success else "⚠️"
        else:
            db_icon = "📝"
        
        if price is not None:
            print(f"    {db_icon} {title[:45]}... - {price} {currency}")
        else:
            print(f"    {db_icon} {title[:45]}... - Fiyat yok")
        
        return product_data
        
    except Exception as e:
        print(f"    ✗ Error: {str(e)[:50]}")
        return None

def scrape_site(config, db_enabled=False):
    """Site scrape et"""
    print(f"\n{'='*70}")
    print(f"🏪 SİTE: {config['name']}")
    print(f"{'='*70}")
    
    products = []
    
    product_sitemaps = get_product_sitemaps(config)
    
    if not product_sitemaps:
        print("✗ Ürün sitemap bulunamadı")
        return products
    
    all_product_urls = []
    for sitemap_url in product_sitemaps:
        print(f"\n📄 Sitemap: {sitemap_url.split('/')[-1]}")
        urls = get_sitemap_urls(sitemap_url)
        
        if urls:
            product_urls = filter_product_urls(urls, config)
            all_product_urls.extend(product_urls)
            print(f"  ✓ {len(product_urls)} ürün URL'si bulundu")
    
    all_product_urls = list(set(all_product_urls))
    
    if TEST_LIMIT > 0:
        print(f"\n⚠️ TEST MODU: Sadece ilk {TEST_LIMIT} ürün işlenecek")
        all_product_urls = all_product_urls[:TEST_LIMIT]
    
    if not all_product_urls:
        print("\n✗ Hiç ürün URL'si bulunamadı")
        return products
    
    print(f"\n📊 Toplam: {len(all_product_urls)} ürün işlenecek")
    print(f"\n⚙️ Ürünler scrape ediliyor...")
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
    print(f"✅ {len(products)} ürün başarıyla scrape edildi")
    
    return products

def main():
    """Ana fonksiyon"""
    print(f"\n{'='*70}")
    print("🚀 ÜRÜN SCRAPER BAŞLATILIYOR (Supabase SDK v3.0)")
    print(f"{'='*70}")
    
    db_enabled = init_supabase()
    
    if db_enabled:
        print("💾 Veriler hem JSON hem Supabase'e kaydedilecek")
    else:
        print("📝 Veriler sadece JSON'a kaydedilecek")
    
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
    
    output_file = 'products.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_products, f, ensure_ascii=False, indent=2)
    
    print(f"\n{'='*70}")
    print("📊 ÖZET İSTATİSTİKLER")
    print(f"{'='*70}")
    print(f"Toplam Ürün: {len(all_products)}")
    
    for site_name, site_stats in stats.items():
        print(f"\n{site_name}:")
        print(f"  • Toplam: {site_stats['total']}")
        print(f"  • Fiyatlı: {site_stats['with_price']}")
        print(f"  • Fiyatsız: {site_stats['without_price']}")
    
    print(f"\n✅ JSON: '{output_file}' dosyasına kaydedildi")
    if db_enabled:
        print(f"✅ Supabase: Tüm ürünler veritabanına kaydedildi")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    main()
,  # slug-ID formatı
        'exclude_patterns': [
            r'/cart', r'/wishlist', r'/category/', r'/checkout', 
            r'/page/', r'/compare', r'/product-\d+-.*-\d+
}

def generate_sku(url, site_name):
    """URL'den benzersiz SKU oluştur"""
    url_part = url.rstrip('/').split('/')[-1]
    site_prefix = site_name[:3].upper()
    url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
    return f"{site_prefix}-{url_part[:30]}-{url_hash}"

def init_supabase():
    """Supabase client'ı başlat"""
    global supabase
    try:
        print("\n🔍 Supabase bağlantısı test ediliyor...")
        print(f"  🔌 REST API ile bağlanılıyor...")
        
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # Test sorgusu
        result = supabase.table('products').select("count", count='exact').execute()
        count = result.count if hasattr(result, 'count') else 0
        
        print(f"  ✅ Bağlantı başarılı!")
        print(f"✅ Veritabanında şu anda {count} ürün var")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Bağlantı hatası: {e}")
        print(f"  ℹ️ Supabase anon key doğru mu kontrol edin")
        return False

def save_product_to_db(product, site_name):
    """Ürünü Supabase'e kaydet (REST API)"""
    try:
        if not supabase:
            return False
        
        sku = generate_sku(product['url'], site_name)
        stock_status = 'in_stock' if product['price'] is not None else 'unknown'
        
        stock_data = {
            'site': site_name,
            'currency': product.get('currency'),
            'last_seen_price': product.get('price'),
            'scraped_at': datetime.now().isoformat()
        }
        
        now = datetime.now().isoformat()
        
        # Upsert (insert or update)
        data = {
            'sku': sku,
            'name': product['title'],
            'price': product['price'],
            'stock_status': stock_status,
            'url': product['url'],
            'product_name': product['title'],
            'product_url': product['url'],
            'stock_data': stock_data,
            'scraped_at': now,
            'updated_at': now
        }
        
        # Upsert: conflict on sku
        supabase.table('products').upsert(data, on_conflict='sku').execute()
        
        return True
        
    except Exception as e:
        print(f"      ❌ DB kayıt hatası: {str(e)[:80]}")
        return False

def get_sitemap_urls(sitemap_url):
    """Sitemap'ten URL'leri çeker"""
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
        print(f"  ✗ Sitemap error: {e}")
        return []

def get_product_sitemaps(config):
    """Sitemap index'ten ürün sitemap'lerini bulur"""
    sitemap_url = config['sitemap_url']
    sitemap_type = config.get('sitemap_type', 'direct')
    
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
    
    print(f"  ✓ {len(product_sitemaps)} ürün sitemap bulundu")
    return product_sitemaps

def filter_product_urls(urls, config):
    """URL'leri filtrele"""
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
    """Fiyat çıkar"""
    for selector in selectors:
        try:
            element = soup.select_one(selector)
            if element:
                if element.name == 'meta':
                    price_text = element.get('content', '')
                else:
                    price_text = element.get_text(strip=True)
                
                price_text = price_text.replace(',', '').replace(' ', '')
                price_match = re.search(r'(\d+\.?\d*)', price_text)
                
                if price_match:
                    try:
                        price = float(price_match.group(1))
                        if price > 0:
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
                if element.name == 'title':
                    title = re.split(r'\s*[|\-]\s*', title)[0]
                
                if title and len(title) > 3:
                    return title
        except:
            continue
    
    return "Bilinmiyor"

def scrape_product(url, config, db_enabled=False):
    """Ürün scrape et"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        title = extract_title(soup, config['selectors']['title'])
        price = extract_price(soup, config['selectors']['price'])
        currency = config['selectors']['currency']
        
        product_data = {
            'title': title,
            'price': price,
            'currency': currency,
            'url': url
        }
        
        if db_enabled:
            db_success = save_product_to_db(product_data, config['name'])
            db_icon = "💾" if db_success else "⚠️"
        else:
            db_icon = "📝"
        
        if price is not None:
            print(f"    {db_icon} {title[:45]}... - {price} {currency}")
        else:
            print(f"    {db_icon} {title[:45]}... - Fiyat yok")
        
        return product_data
        
    except Exception as e:
        print(f"    ✗ Error: {str(e)[:50]}")
        return None

def scrape_site(config, db_enabled=False):
    """Site scrape et"""
    print(f"\n{'='*70}")
    print(f"🏪 SİTE: {config['name']}")
    print(f"{'='*70}")
    
    products = []
    
    product_sitemaps = get_product_sitemaps(config)
    
    if not product_sitemaps:
        print("✗ Ürün sitemap bulunamadı")
        return products
    
    all_product_urls = []
    for sitemap_url in product_sitemaps:
        print(f"\n📄 Sitemap: {sitemap_url.split('/')[-1]}")
        urls = get_sitemap_urls(sitemap_url)
        
        if urls:
            product_urls = filter_product_urls(urls, config)
            all_product_urls.extend(product_urls)
            print(f"  ✓ {len(product_urls)} ürün URL'si bulundu")
    
    all_product_urls = list(set(all_product_urls))
    
    if TEST_LIMIT > 0:
        print(f"\n⚠️ TEST MODU: Sadece ilk {TEST_LIMIT} ürün işlenecek")
        all_product_urls = all_product_urls[:TEST_LIMIT]
    
    if not all_product_urls:
        print("\n✗ Hiç ürün URL'si bulunamadı")
        return products
    
    print(f"\n📊 Toplam: {len(all_product_urls)} ürün işlenecek")
    print(f"\n⚙️ Ürünler scrape ediliyor...")
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
    print(f"✅ {len(products)} ürün başarıyla scrape edildi")
    
    return products

def main():
    """Ana fonksiyon"""
    print(f"\n{'='*70}")
    print("🚀 ÜRÜN SCRAPER BAŞLATILIYOR (Supabase SDK v3.0)")
    print(f"{'='*70}")
    
    db_enabled = init_supabase()
    
    if db_enabled:
        print("💾 Veriler hem JSON hem Supabase'e kaydedilecek")
    else:
        print("📝 Veriler sadece JSON'a kaydedilecek")
    
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
    
    output_file = 'products.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_products, f, ensure_ascii=False, indent=2)
    
    print(f"\n{'='*70}")
    print("📊 ÖZET İSTATİSTİKLER")
    print(f"{'='*70}")
    print(f"Toplam Ürün: {len(all_products)}")
    
    for site_name, site_stats in stats.items():
        print(f"\n{site_name}:")
        print(f"  • Toplam: {site_stats['total']}")
        print(f"  • Fiyatlı: {site_stats['with_price']}")
        print(f"  • Fiyatsız: {site_stats['without_price']}")
    
    print(f"\n✅ JSON: '{output_file}' dosyasına kaydedildi")
    if db_enabled:
        print(f"✅ Supabase: Tüm ürünler veritabanına kaydedildi")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    main()
  # eski format
        ],
        'selectors': {
            'title': [
                'h1[itemprop="name"]',
                '.product-title h1',
                'h1.product-name',
                '.oe_product h1',
                'h1',
                'title'
            ],
            'price': [
                'span[itemprop="price"]',
                '.oe_currency_value',
                'span.oe_price',
                '.product_price span',
                'div[class*="price"] span',
                'meta[property="product:price:amount"]'
            ],
            'currency': 'USD'
        }
    }
}

def generate_sku(url, site_name):
    """URL'den benzersiz SKU oluştur"""
    url_part = url.rstrip('/').split('/')[-1]
    site_prefix = site_name[:3].upper()
    url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
    return f"{site_prefix}-{url_part[:30]}-{url_hash}"

def init_supabase():
    """Supabase client'ı başlat"""
    global supabase
    try:
        print("\n🔍 Supabase bağlantısı test ediliyor...")
        print(f"  🔌 REST API ile bağlanılıyor...")
        
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # Test sorgusu
        result = supabase.table('products').select("count", count='exact').execute()
        count = result.count if hasattr(result, 'count') else 0
        
        print(f"  ✅ Bağlantı başarılı!")
        print(f"✅ Veritabanında şu anda {count} ürün var")
        
        return True
        
    except Exception as e:
        print(f"  ❌ Bağlantı hatası: {e}")
        print(f"  ℹ️ Supabase anon key doğru mu kontrol edin")
        return False

def save_product_to_db(product, site_name):
    """Ürünü Supabase'e kaydet (REST API)"""
    try:
        if not supabase:
            return False
        
        sku = generate_sku(product['url'], site_name)
        stock_status = 'in_stock' if product['price'] is not None else 'unknown'
        
        stock_data = {
            'site': site_name,
            'currency': product.get('currency'),
            'last_seen_price': product.get('price'),
            'scraped_at': datetime.now().isoformat()
        }
        
        now = datetime.now().isoformat()
        
        # Upsert (insert or update)
        data = {
            'sku': sku,
            'name': product['title'],
            'price': product['price'],
            'stock_status': stock_status,
            'url': product['url'],
            'product_name': product['title'],
            'product_url': product['url'],
            'stock_data': stock_data,
            'scraped_at': now,
            'updated_at': now
        }
        
        # Upsert: conflict on sku
        supabase.table('products').upsert(data, on_conflict='sku').execute()
        
        return True
        
    except Exception as e:
        print(f"      ❌ DB kayıt hatası: {str(e)[:80]}")
        return False

def get_sitemap_urls(sitemap_url):
    """Sitemap'ten URL'leri çeker"""
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
        print(f"  ✗ Sitemap error: {e}")
        return []

def get_product_sitemaps(config):
    """Sitemap index'ten ürün sitemap'lerini bulur"""
    sitemap_url = config['sitemap_url']
    sitemap_type = config.get('sitemap_type', 'direct')
    
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
    
    print(f"  ✓ {len(product_sitemaps)} ürün sitemap bulundu")
    return product_sitemaps

def filter_product_urls(urls, config):
    """URL'leri filtrele"""
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
    """Fiyat çıkar"""
    for selector in selectors:
        try:
            element = soup.select_one(selector)
            if element:
                if element.name == 'meta':
                    price_text = element.get('content', '')
                else:
                    price_text = element.get_text(strip=True)
                
                price_text = price_text.replace(',', '').replace(' ', '')
                price_match = re.search(r'(\d+\.?\d*)', price_text)
                
                if price_match:
                    try:
                        price = float(price_match.group(1))
                        if price > 0:
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
                if element.name == 'title':
                    title = re.split(r'\s*[|\-]\s*', title)[0]
                
                if title and len(title) > 3:
                    return title
        except:
            continue
    
    return "Bilinmiyor"

def scrape_product(url, config, db_enabled=False):
    """Ürün scrape et"""
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        title = extract_title(soup, config['selectors']['title'])
        price = extract_price(soup, config['selectors']['price'])
        currency = config['selectors']['currency']
        
        product_data = {
            'title': title,
            'price': price,
            'currency': currency,
            'url': url
        }
        
        if db_enabled:
            db_success = save_product_to_db(product_data, config['name'])
            db_icon = "💾" if db_success else "⚠️"
        else:
            db_icon = "📝"
        
        if price is not None:
            print(f"    {db_icon} {title[:45]}... - {price} {currency}")
        else:
            print(f"    {db_icon} {title[:45]}... - Fiyat yok")
        
        return product_data
        
    except Exception as e:
        print(f"    ✗ Error: {str(e)[:50]}")
        return None

def scrape_site(config, db_enabled=False):
    """Site scrape et"""
    print(f"\n{'='*70}")
    print(f"🏪 SİTE: {config['name']}")
    print(f"{'='*70}")
    
    products = []
    
    product_sitemaps = get_product_sitemaps(config)
    
    if not product_sitemaps:
        print("✗ Ürün sitemap bulunamadı")
        return products
    
    all_product_urls = []
    for sitemap_url in product_sitemaps:
        print(f"\n📄 Sitemap: {sitemap_url.split('/')[-1]}")
        urls = get_sitemap_urls(sitemap_url)
        
        if urls:
            product_urls = filter_product_urls(urls, config)
            all_product_urls.extend(product_urls)
            print(f"  ✓ {len(product_urls)} ürün URL'si bulundu")
    
    all_product_urls = list(set(all_product_urls))
    
    if TEST_LIMIT > 0:
        print(f"\n⚠️ TEST MODU: Sadece ilk {TEST_LIMIT} ürün işlenecek")
        all_product_urls = all_product_urls[:TEST_LIMIT]
    
    if not all_product_urls:
        print("\n✗ Hiç ürün URL'si bulunamadı")
        return products
    
    print(f"\n📊 Toplam: {len(all_product_urls)} ürün işlenecek")
    print(f"\n⚙️ Ürünler scrape ediliyor...")
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
    print(f"✅ {len(products)} ürün başarıyla scrape edildi")
    
    return products

def main():
    """Ana fonksiyon"""
    print(f"\n{'='*70}")
    print("🚀 ÜRÜN SCRAPER BAŞLATILIYOR (Supabase SDK v3.0)")
    print(f"{'='*70}")
    
    db_enabled = init_supabase()
    
    if db_enabled:
        print("💾 Veriler hem JSON hem Supabase'e kaydedilecek")
    else:
        print("📝 Veriler sadece JSON'a kaydedilecek")
    
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
    
    output_file = 'products.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_products, f, ensure_ascii=False, indent=2)
    
    print(f"\n{'='*70}")
    print("📊 ÖZET İSTATİSTİKLER")
    print(f"{'='*70}")
    print(f"Toplam Ürün: {len(all_products)}")
    
    for site_name, site_stats in stats.items():
        print(f"\n{site_name}:")
        print(f"  • Toplam: {site_stats['total']}")
        print(f"  • Fiyatlı: {site_stats['with_price']}")
        print(f"  • Fiyatsız: {site_stats['without_price']}")
    
    print(f"\n✅ JSON: '{output_file}' dosyasına kaydedildi")
    if db_enabled:
        print(f"✅ Supabase: Tüm ürünler veritabanına kaydedildi")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    main()
