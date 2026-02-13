#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Ürün Scraper v3.3 - Database URL Source"""

import requests
from bs4 import BeautifulSoup
import json
import time
import re
from supabase import create_client
from datetime import datetime
from urllib.parse import urlparse
import hashlib

TEST_LIMIT = 0  # 0 = Tüm ürünleri scrape et

SUPABASE_URL = "https://zmmpuysxnwqngvlafolm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InptbXB1eXN4bndxbmd2bGFmb2xtIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjkwNjA0MTAsImV4cCI6MjA4NDYzNjQxMH0.4Q7k-cDcaGhOurMlofG8lkd4ApPyYexxkMdXxH-lI0k"

supabase = None

SITE_CONFIGS = {
    'technopluskibris.com': {
        'name': 'TECHNOPLUSKIBRIS',
        'selectors': {
            'title': ['h1.product-name', 'h1.product-title', '.product-detail h1', 'h1', 'title'],
            'price': ['.product-price span', '.product-price', 'span[class*="price"]', 'div[class*="price"]'],
            'currency': 'TL'
        }
    },
    'durmazz.com': {
        'name': 'DURMAZZ',
        'selectors': {
            'title': ['h1[itemprop="name"]', '.product-title h1', 'h1.product-name', 'h1', 'title'],
            'price': ['span[itemprop="price"]', '.oe_currency_value', 'span.oe_price', '.product_price span'],
            'currency': 'USD'
        }
    },
    'irismostore.com': {
        'name': 'IRISMOSTORE',
        'selectors': {
            'title': [
                'h1.productDetail-title',
                'h1.product-name',
                '.product-detail-name',
                'h1[itemprop="name"]',
                'h1',
                '.product-title',
                'title'
            ],
            'price': [
                'span.productDetail-price',
                'div.productDetail-price span',
                '.product-price-value',
                'span.price-value',
                'div.price span',
                'span[class*="price"]',
                '.product-price',
                'h3',
                'span.price',
                'meta[property="product:price:amount"]'
            ],
            'currency': 'USD'  # USD olarak değiştirildi
        }
    },
    'sharafstore.com': {
        'name': 'SHARAFSTORE',
        'selectors': {
            'title': ['h1.product-title', 'h1[itemprop="name"]', '.product-name', 'h1', 'title'],
            'price': [
                'span.price',
                '.product-price span',
                'span[class*="price"]',
                'div.price',
                '.price-wrapper span'
            ],
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

def get_urls_from_database():
    """productofsitemapcrawl tablosundan URL'leri çeker (processed olmayanlar)"""
    try:
        print("\n📥 Veritabanından URL'ler çekiliyor...")
        
        # Önce processed=false olanları kontrol et
        response = supabase.table('productofsitemapcrawl')\
            .select('id, url, anawebsite, processed')\
            .or_('processed.is.null,processed.eq.false')\
            .execute()
        
        # Eğer processed kolonu yoksa, tüm URL'leri çek
        if not response.data:
            print("  ℹ️ Processed flag yok veya tüm URL'ler işlenmiş, tüm kayıtlar çekiliyor...")
            response = supabase.table('productofsitemapcrawl')\
                .select('id, url, anawebsite')\
                .execute()
        
        if response.data:
            processed_count = len([r for r in response.data if r.get('processed') == True])
            unprocessed_count = len(response.data) - processed_count
            
            print(f"  ✅ {len(response.data)} URL bulundu")
            print(f"     └─ İşlenmemiş: {unprocessed_count}")
            if processed_count > 0:
                print(f"     └─ İşlenmiş: {processed_count} (atlandı)")
            
            return response.data
        else:
            print("  ⚠️ Veritabanında URL bulunamadı")
            return []
    except Exception as e:
        print(f"  ❌ Hata: {e}")
        print(f"     └─ Tüm URL'ler çekiliyor (fallback)...")
        
        # Hata durumunda tüm URL'leri çek
        try:
            response = supabase.table('productofsitemapcrawl')\
                .select('id, url, anawebsite')\
                .execute()
            
            if response.data:
                print(f"  ✅ {len(response.data)} URL bulundu (fallback)")
                return response.data
        except:
            pass
        
        return []

def get_site_config_from_url(url):
    """URL'den site config'ini belirler"""
    domain = urlparse(url).netloc.replace('www.', '')
    
    for config_domain, config in SITE_CONFIGS.items():
        if config_domain in domain:
            return config
    
    # Default config (eğer tanımlı değilse)
    return {
        'name': domain.upper().replace('.', ''),
        'selectors': {
            'title': ['h1', '.product-title', 'title'],
            'price': ['.price', 'span.price', '.product-price'],
            'currency': 'TL'
        }
    }

def save_product_to_db(product, site_name):
    try:
        if not supabase:
            return False
        
        sku = generate_sku(product['url'], site_name)
        new_price = product['price']
        
        # Mevcut ürünü kontrol et
        existing = supabase.table('products').select('price, previous_price, price_change').eq('sku', sku).execute()
        
        old_price = None
        previous_price = None
        price_change = None
        price_changed_at = None
        is_new_product = False
        
        if existing.data and len(existing.data) > 0:
            # Ürün VAR - güncelleme yapılacak
            old_price = existing.data[0].get('price')
            
            if old_price is not None and new_price is not None:
                # İki fiyat da var, karşılaştır
                old_price_float = float(old_price)
                new_price_float = float(new_price)
                
                if old_price_float != new_price_float:
                    # 🎯 FİYAT DEĞİŞTİ!
                    previous_price = old_price
                    price_change = new_price_float - old_price_float
                    price_changed_at = datetime.now().isoformat()
                    
                    change_type = "📈 ARTTI" if price_change > 0 else "📉 DÜŞTÜ"
                    print(f"      💰 {change_type}: {old_price} → {new_price} ({price_change:+.2f})")
                else:
                    # Fiyat aynı - önceki değerleri koru
                    previous_price = existing.data[0].get('previous_price')
                    price_change = existing.data[0].get('price_change')
                    # price_changed_at güncelleme (önceki değeri koru)
            elif new_price is not None:
                # Önceden fiyat yoktu, şimdi var
                print(f"      ℹ️ Fiyat eklendi: {new_price}")
            else:
                # Yeni fiyat yok - önceki değerleri koru
                previous_price = existing.data[0].get('previous_price')
                price_change = existing.data[0].get('price_change')
        else:
            # Ürün YOK - yeni ürün eklenecek
            is_new_product = True
            if new_price is not None:
                print(f"      ✨ Yeni ürün: {new_price}")
        
        # Stock status
        stock_status = 'in_stock' if new_price is not None else 'unknown'
        
        # Data hazırla
        data = {
            'sku': sku,
            'name': product['title'],
            'price': new_price,
            'previous_price': previous_price,
            'price_change': price_change,
            'price_changed_at': price_changed_at,
            'stock_status': stock_status,
            'url': product['url'],
            'product_name': product['title'],
            'product_url': product['url'],
            'stock_data': {
                'site': site_name,
                'currency': product.get('currency'),
                'last_seen_price': new_price,
                'scraped_at': datetime.now().isoformat(),
                'is_new_product': is_new_product,
                'price_history': {
                    'old': str(old_price) if old_price else None,
                    'new': str(new_price) if new_price else None,
                    'change': str(price_change) if price_change else None
                }
            },
            'scraped_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        
        # Veritabanına kaydet/güncelle
        result = supabase.table('products').upsert(data, on_conflict='sku').execute()
        
        # Debug: Kaydın başarılı olduğunu kontrol et
        if result.data:
            # Sessiz başarı (sadece fiyat değişirse mesaj göster)
            return True
        else:
            print(f"      ⚠️ Upsert sonucu boş döndü (SKU: {sku})")
            return False
            
    except Exception as e:
        print(f"      ❌ DB Hatası: {str(e)}")
        return False

def mark_url_as_processed(url_id, success=True):
    """URL'yi processed olarak işaretler"""
    try:
        data = {
            'processed': True,
            'processed_at': datetime.now().isoformat()
        }
        
        # last_scrape_status kolonunu kaldırdık (tabloda yok)
        
        supabase.table('productofsitemapcrawl')\
            .update(data)\
            .eq('id', url_id)\
            .execute()
        
        return True
    except Exception as e:
        # Processed kolonu yoksa sessizce devam et
        if 'column' in str(e).lower() and 'processed' in str(e).lower():
            return True  # Kolon yok, sorun değil
        
        print(f"      ⚠️ İşaretleme hatası: {str(e)[:50]}")
        return False

def extract_price(soup, selectors):
    for selector in selectors:
        try:
            element = soup.select_one(selector)
            if element:
                price_text = None
                
                # 1. Meta tag ise content attribute'undan al
                if element.name == 'meta':
                    price_text = element.get('content', '')
                
                # 2. Data attribute'lerden dene (data-price, data-value vs)
                elif not price_text:
                    for attr in ['data-price', 'data-value', 'data-product-price', 'content']:
                        if element.get(attr):
                            price_text = element.get(attr)
                            break
                
                # 3. Text içeriğinden al
                if not price_text:
                    price_text = element.get_text(strip=True)
                
                if not price_text:
                    continue
                
                # Fiyat parse
                # Virgül ve boşlukları temizle
                price_text = price_text.replace(',', '').replace(' ', '').replace('\n', '').replace('\t', '')
                
                # Para birimi sembollerini kaldır (TL, USD, $, € vb)
                price_text = price_text.replace('TL', '').replace('USD', '').replace('$', '').replace('€', '')
                
                # Sadece rakam ve nokta bırak
                price_text = re.sub(r'[^\d.]', '', price_text)
                
                if price_text:
                    try:
                        price = float(price_text)
                        if price > 0:
                            return price
                    except:
                        continue
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
        
        # DEBUG: Fiyat bulunamazsa HTML'den ipucu bul
        if price is None and 'irismostore' in url.lower():
            # Fiyat olabilecek tüm elementleri bul
            potential_prices = []
            
            # Tüm span, div, meta'ları tara
            for elem in soup.find_all(['span', 'div', 'meta', 'p']):
                text = elem.get_text(strip=True) if elem.name != 'meta' else elem.get('content', '')
                # $ veya rakam içeren elementleri bul
                if text and ('$' in text or any(char.isdigit() for char in text)):
                    if len(text) < 50:  # Çok uzun textleri alma
                        potential_prices.append(f"{elem.name}.{elem.get('class', [''])[0] if elem.get('class') else ''}: {text[:30]}")
            
            if potential_prices:
                print(f"\n      🔍 Fiyat bulunamadı, potansiyel elementler:")
                for p in potential_prices[:5]:  # İlk 5'ini göster
                    print(f"         {p}")
        
        product_data = {'title': title, 'price': price, 'currency': currency, 'url': url}
        
        if db_enabled:
            db_success = save_product_to_db(product_data, config['name'])
            db_icon = "💾" if db_success else "⚠️"
        else:
            db_icon = "📝"
        
        # Daha detaylı log
        price_str = f"{price} {currency}" if price else "❌ Fiyat yok"
        title_short = title[:40] + "..." if len(title) > 40 else title
        
        print(f"    {db_icon} {title_short} - {price_str}")
        
        # Fiyat yoksa URL'yi de göster (debug için)
        if price is None:
            print(f"       └─ URL: {url[:70]}...")
        
        return product_data
    except requests.exceptions.Timeout:
        print(f"    ⏱️ Timeout: {url[:50]}...")
        return None
    except requests.exceptions.RequestException as e:
        print(f"    ✗ Network Error: {str(e)[:40]}")
        return None
    except Exception as e:
        print(f"    ✗ Parse Error: {str(e)[:40]}")
        print(f"       └─ URL: {url[:60]}...")
        return None

def scrape_from_database(db_enabled=False):
    """Veritabanından URL'leri çekip scrape eder"""
    print(f"\n{'='*70}")
    print(f"🗄️ VERİTABANINDAN SCRAPING")
    print(f"{'='*70}")
    
    # URL'leri veritabanından çek
    url_records = get_urls_from_database()
    
    if not url_records:
        print("✗ İşlenecek URL bulunamadı")
        return []
    
    # Processed olanları filtrele
    unprocessed_records = [r for r in url_records if not r.get('processed')]
    
    if not unprocessed_records:
        print("⚠️ Tüm URL'ler zaten işlenmiş!")
        print("💡 İpucu: Yeniden scrape etmek için SQL çalıştırın:")
        print("   UPDATE productofsitemapcrawl SET processed = false;")
        return []
    
    if TEST_LIMIT > 0:
        print(f"\n⚠️ TEST: İlk {TEST_LIMIT} URL")
        unprocessed_records = unprocessed_records[:TEST_LIMIT]
    
    products = []
    site_stats = {}
    
    print(f"\n📊 {len(unprocessed_records)} URL scrape edilecek")
    print(f"{'─'*70}")
    
    for i, record in enumerate(unprocessed_records, 1):
        url_id = record.get('id')
        url = record.get('url')
        ana_website = record.get('anawebsite', '')
        
        if not url:
            continue
        
        # Site config'ini belirle
        config = get_site_config_from_url(url)
        site_name = config['name']
        
        # Site istatistiklerini başlat
        if site_name not in site_stats:
            site_stats[site_name] = {'total': 0, 'success': 0, 'failed': 0}
        
        print(f"  [{i}/{len(unprocessed_records)}] {site_name}", end=" ")
        
        product = scrape_product(url, config, db_enabled)
        
        if product:
            product['site'] = site_name
            product['anawebsite'] = ana_website
            products.append(product)
            site_stats[site_name]['success'] += 1
            
            # Başarılı - processed olarak işaretle
            if url_id:
                mark_url_as_processed(url_id, success=True)
        else:
            site_stats[site_name]['failed'] += 1
            
            # Başarısız - yine de işaretle (tekrar denemesin)
            if url_id:
                mark_url_as_processed(url_id, success=False)
        
        site_stats[site_name]['total'] += 1
        
        # Rate limiting
        if i % 10 == 0:
            time.sleep(1)
        else:
            time.sleep(0.3)
    
    print(f"{'─'*70}")
    print(f"✅ {len(products)} ürün tamamlandı")
    
    # Site bazlı özet
    print(f"\n📊 Site Bazlı Özet:")
    for site_name, stats in site_stats.items():
        success_rate = (stats['success'] / stats['total'] * 100) if stats['total'] > 0 else 0
        print(f"  {site_name}: {stats['success']}/{stats['total']} başarılı ({success_rate:.1f}%)")
        if stats['failed'] > 0:
            print(f"     └─ Başarısız: {stats['failed']}")
    
    return products

def main():
    print(f"\n{'='*70}")
    print("🚀 SCRAPER v3.3 - DATABASE URL SOURCE")
    print(f"{'='*70}")
    
    db_enabled = init_supabase()
    
    if db_enabled:
        print("💾 JSON + Supabase")
    else:
        print("📝 Sadece JSON")
    
    # Veritabanından scrape et
    all_products = scrape_from_database(db_enabled)
    
    # JSON'a kaydet
    with open('products.json', 'w', encoding='utf-8') as f:
        json.dump(all_products, f, ensure_ascii=False, indent=2)
    
    # Özet istatistikler
    print(f"\n{'='*70}")
    print("📊 GENEL ÖZET")
    print(f"{'='*70}")
    print(f"Toplam: {len(all_products)}")
    
    total_with_price = len([p for p in all_products if p.get('price') is not None])
    total_without_price = len(all_products) - total_with_price
    
    print(f"Fiyatlı: {total_with_price}")
    print(f"Fiyatsız: {total_without_price}")
    
    print(f"\n✅ products.json kaydedildi")
    if db_enabled:
        print(f"✅ Supabase güncellendi")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    main()
