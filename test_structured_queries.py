import os
import sys
from typing import Dict, Any
import json
from bson import ObjectId

# Add your project root to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import your modules (adjust imports based on your project structure)
try:
    from database.connection import get_database
    from database.queries import (
        create_search_indexes, 
        search_content_by_text,
        fetch_content_with_complex_filters,
        fetch_distribution_analysis,
        fetch_content_gap_analysis,
        fetch_content,
        fetch_content_by_filters
    )
    from database.extractor import DynamicTenantSchemaExtractor
    from utils.logger import get_logger
except ImportError as e:
    print(f"Import error: {e}")
    print("Please adjust the import paths based on your project structure")
    sys.exit(1)

# Configuration
TENANT_ID = "6875f3afc8337606d54a7f37"  # Your demo tenant ID
logger = get_logger("test_search")

def print_separator(title: str):
    """Print a nice separator for test sections"""
    print(f"\n{'='*60}")
    print(f" {title}")
    print(f"{'='*60}")

def print_results(results: Dict[str, Any], limit: int = 3):
    """Pretty print search results"""
    if "error" in results:
        print(f"❌ Error: {results['error']}")
        return
    
    if "results" in results:
        print(f"✅ Found {results.get('total_found', 0)} results")
        for i, result in enumerate(results["results"][:limit]):
            print(f"\n{i+1}. {result.get('title', 'No title')}")
            print(f"   ID: {result.get('_id')}")
            print(f"   Relevance: {result.get('relevance_score', 0)}")
            print(f"   Match Reason: {result.get('match_reason', 'unknown')}")
            if result.get('matched_tags'):
                print(f"   Matched Tags: {result.get('matched_tags')}")
            if result.get('matched_category_value'):
                print(f"   Matched Category: {result.get('matched_category_value')}")
            print(f"   Description: {result.get('description', 'No description')[:100]}...")
    elif "data" in results:
        print(f"✅ Found {len(results['data'])} results")
        for i, result in enumerate(results["data"][:limit]):
            print(f"\n{i+1}. {result.get('title', 'No title')}")
            print(f"   ID: {result.get('_id')}")
    else:
        print("✅ Results:", json.dumps(results, indent=2, default=str)[:500])

def test_database_connection():
    """Test basic database connection"""
    print_separator("Testing Database Connection")
    try:
        db = get_database()
        # Test basic query
        count = db.sitemaps.count_documents({"tenant": ObjectId(TENANT_ID)})
        print(f"✅ Database connected successfully")
        print(f"✅ Found {count} sitemaps for tenant {TENANT_ID}")
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def test_schema_extraction():
    """Test schema extraction"""
    print_separator("Testing Schema Extraction")
    try:
        extractor = DynamicTenantSchemaExtractor(TENANT_ID)
        schema = extractor.extract_schema()
        print(f"✅ Schema extracted successfully")
        print(f"   Categories: {list(schema.categories.keys())}")
        print(f"   Field Mappings: {list(schema.field_mappings.keys())}")
        return schema
    except Exception as e:
        print(f"❌ Schema extraction failed: {e}")
        return None

def test_index_creation():
    """Test creating search indexes"""
    print_separator("Testing Search Index Creation")
    try:
        db = get_database()
        create_search_indexes(db)
        print("✅ Search indexes created successfully")
        
        # List existing indexes
        indexes = list(db.sitemaps.list_indexes())
        print(f"✅ Sitemaps collection has {len(indexes)} indexes:")
        for idx in indexes:
            print(f"   - {idx.get('name', 'unnamed')}")
        return True
    except Exception as e:
        print(f"❌ Index creation failed: {e}")
        return False

def test_basic_content_fetch():
    """Test basic content fetching"""
    print_separator("Testing Basic Content Fetch")
    try:
        results = fetch_content(TENANT_ID, limit=5)
        print(f"✅ Fetched {len(results)} content items")
        if results:
            sample = results[0]
            print(f"✅ Sample fields: {list(sample.keys())}")
            print(f"✅ Sample title: {sample.get('title', 'No title')}")
        return True
    except Exception as e:
        print(f"❌ Basic fetch failed: {e}")
        return False

def test_text_search():
    """Test text-based search functionality"""
    print_separator("Testing Text Search")
    
    test_queries = [
        "investment",
        "Public.com", 
        "TOFU",
        "Financial Services",
        "crypto"
    ]
    
    for query in test_queries:
        print(f"\n🔍 Searching for: '{query}'")
        try:
            results = search_content_by_text(
                tenant_id=TENANT_ID,
                search_query=query,
                include_tag_search=True,
                include_category_search=True,
                limit=3
            )
            print_results(results, limit=2)
        except Exception as e:
            print(f"❌ Search failed for '{query}': {e}")

def test_filtered_search():
    """Test filtered content search"""
    print_separator("Testing Filtered Search")
    
    filter_tests = [
        {"Funnel Stage": "TOFU"},
        {"Primary Audience": "Individual Investors"},
        {"Language": "English"}
    ]
    
    for filters in filter_tests:
        print(f"\n🔍 Filtering by: {filters}")
        try:
            results = fetch_content_by_filters(
                tenant_id=TENANT_ID,
                filters=filters,
                # limit=3
            )
            print(f"✅ Found {len(results)} results")
            # if results:
            #     for i, result in enumerate(results[:2]):
            #         print(f"   {i+1}. {result.get('title', 'No title')}")
            #         print(f"      Filter match: {filters}")
        except Exception as e:
            print(f"❌ Filtered search failed: {e}")

def test_complex_filters():
    """Test complex multi-category filtering"""
    print_separator("Testing Complex Multi-Category Filtering")
    
    try:
        results = fetch_content_with_complex_filters(
            tenant_id=TENANT_ID,
            category_filters={
                "Funnel Stage": ["TOFU"],
                "Primary Audience": ["Individual Investors"]
            },
            additional_filters={},
            limit=5
        )
        print_results(results)
    except Exception as e:
        print(f"❌ Complex filtering failed: {e}")

def test_distribution_analysis():
    """Test distribution analysis"""
    print_separator("Testing Distribution Analysis")
    
    try:
        results = fetch_distribution_analysis(
            tenant_id=TENANT_ID,
            primary_field="Funnel Stage",
            include_examples=True
        )

        print("✅ Distribution Analysis Results")
        print(f"Total analyzed: {results['total_analyzed']}")
        print(f"Field analyzed: {results['field_analyzed']}")
        if results.get("secondary_field"):
            print(f"Secondary field: {results['secondary_field']}")

        for dist in results["distribution"]:
            print(f"\nCategory: {dist['_id']}")
            print(f"Count: {dist['count']}")
            
            # Show only top 3 examples for readability
            if "examples" in dist:
                example_count = len(dist["examples"])
                examples_to_show = dist["examples"][:3]
                print("Examples:")
                for ex in examples_to_show:
                    print(f"  - ID: {ex['id']} | Title: {ex.get('title', 'N/A')}")
                if example_count > 3:
                    print(f"  ... and {example_count - 3} more")

    except Exception as e:
        print(f"❌ Distribution analysis failed: {e}")


def test_gap_analysis():
    """Test content gap analysis"""
    print_separator("Testing Content Gap Analysis")
    
    try:
        results = fetch_content_gap_analysis(
            tenant_id=TENANT_ID,
            primary_dimension="Funnel Stage"
        )
        print_results(results)
    except Exception as e:
        print(f"❌ Gap analysis failed: {e}")

def run_sample_data_check():
    """Check what sample data exists"""
    print_separator("Sample Data Check")
    try:
        db = get_database()
        
        # Check sitemaps
        sample_sitemap = db.sitemaps.find_one({"tenant": ObjectId(TENANT_ID)})
        if sample_sitemap:
            print("✅ Sample sitemap found:")
            print(f"   Title: {sample_sitemap.get('name', 'No name')}")
            print(f"   Description: {sample_sitemap.get('description', 'No description')[:100]}...")
            print(f"   Fields: {list(sample_sitemap.keys())}")
        
        # Check tags
        tags_count = db.custom_tags.count_documents({"tenant": ObjectId(TENANT_ID)})
        print(f"✅ Found {tags_count} custom tags")
        
        # Check category attributes  
        cat_attrs_count = db.category_attributes.count_documents({"tenant": ObjectId(TENANT_ID)})
        print(f"✅ Found {cat_attrs_count} category attributes")
        
    except Exception as e:
        print(f"❌ Sample data check failed: {e}")

def main():
    """Run all tests"""
    print("🚀 Starting Database Search Function Tests")
    print(f"Using Tenant ID: {TENANT_ID}")
    
    # Step 1: Basic connectivity
    if not test_database_connection():
        print("❌ Cannot proceed without database connection")
        return
    
    # Step 2: Sample data check
    run_sample_data_check()
    
    # Step 3: Schema extraction
    schema = test_schema_extraction()
    if not schema:
        print("❌ Cannot proceed without schema")
        return
    
    # Step 4: Index creation
    test_index_creation()
    
    # Step 5: Basic functionality
    test_basic_content_fetch()
    
    # Step 6: Search tests
    test_text_search()
    test_filtered_search()
    test_complex_filters()
    
    # Step 7: Analytics tests
    test_distribution_analysis()
    test_gap_analysis()
    
    print_separator("Test Summary")
    print("✅ All tests completed!")
    print("📝 Check the output above for any errors or issues")
    print("🔧 If you see import errors, adjust the import paths in this script")

if __name__ == "__main__":
    main()