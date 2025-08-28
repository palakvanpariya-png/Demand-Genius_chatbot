from typing import Dict, List, Any, Optional, Union
import logging
from collections import Counter, defaultdict

class AnalyticsEngine:
    """
    Simple, general-purpose analytics engine for any data structure.
    Two core functions that work with any MongoDB results.
    """
    
    def calculate_summary_stats(self, data: List[Dict], fields: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Calculate basic statistics for any numeric fields in the data.
        
        Args:
            data: List of documents from MongoDB
            fields: Optional list of field names to analyze. If None, auto-discovers numeric fields
        
        Returns:
            Dictionary with summary statistics for each field
        """
        if not data:
            return {"error": "No data provided", "total_documents": 0}
        
        try:
            # Auto-discover numeric fields if not specified
            if fields is None:
                fields = self._discover_numeric_fields(data)
            
            stats = {
                "total_documents": len(data),
                "fields_analyzed": fields,
                "summary": {}
            }
            
            for field in fields:
                field_values = self._extract_field_values(data, field)
                
                if field_values:
                    stats["summary"][field] = {
                        "count": len(field_values),
                        "sum": sum(field_values),
                        "average": sum(field_values) / len(field_values),
                        "min": min(field_values),
                        "max": max(field_values),
                        "non_null_percentage": (len(field_values) / len(data)) * 100
                    }
                else:
                    stats["summary"][field] = {
                        "count": 0,
                        "message": "No valid numeric values found"
                    }
            
            return stats
            
        except Exception as e:
            logging.error(f"Error calculating summary stats: {e}")
            return {"error": str(e), "total_documents": len(data)}
    
    def analyze_distribution(self, data: List[Dict], group_by_field: str, value_field: Optional[str] = None) -> Dict[str, Any]:
        """
        Analyze distribution of data by any field.
        
        Args:
            data: List of documents from MongoDB
            group_by_field: Field to group by (e.g., "_id", "categoryAttribute")
            value_field: Optional field to sum/aggregate (e.g., "count", "wordCount")
        
        Returns:
            Dictionary with distribution analysis
        """
        if not data:
            return {"error": "No data provided", "total_documents": 0}
        
        try:
            # Extract grouping values
            groups = self._extract_field_values(data, group_by_field, convert_to_string=True)
            
            if not groups:
                return {
                    "error": f"No valid values found for field '{group_by_field}'",
                    "total_documents": len(data)
                }
            
            # Calculate distribution
            if value_field:
                # Aggregate by value field (sum values for each group)
                distribution = self._calculate_value_distribution(data, group_by_field, value_field)
            else:
                # Simple count distribution
                distribution = Counter(groups)
            
            # Calculate percentages and sort
            total = sum(distribution.values())
            distribution_with_percentages = [
                {
                    "category": str(category),
                    "value": count,
                    "percentage": round((count / total) * 100, 2) if total > 0 else 0
                }
                for category, count in distribution.most_common()
            ]
            
            return {
                "total_documents": len(data),
                "group_by_field": group_by_field,
                "value_field": value_field,
                "total_value": total,
                "unique_categories": len(distribution),
                "distribution": distribution_with_percentages
            }
            
        except Exception as e:
            logging.error(f"Error analyzing distribution: {e}")
            return {"error": str(e), "total_documents": len(data)}
    
    def _discover_numeric_fields(self, data: List[Dict]) -> List[str]:
        """Auto-discover numeric fields from the data"""
        if not data:
            return []
        
        numeric_fields = []
        
        # Sample first few documents to find numeric fields
        sample_size = min(5, len(data))
        sample_docs = data[:sample_size]
        
        # Get all possible field names
        all_fields = set()
        for doc in sample_docs:
            all_fields.update(doc.keys())
        
        # Check which fields are consistently numeric
        for field in all_fields:
            numeric_count = 0
            total_count = 0
            
            for doc in sample_docs:
                if field in doc:
                    total_count += 1
                    if isinstance(doc[field], (int, float)) and not isinstance(doc[field], bool):
                        numeric_count += 1
            
            # If more than 50% of values are numeric, consider it a numeric field
            if total_count > 0 and (numeric_count / total_count) > 0.5:
                numeric_fields.append(field)
        
        return numeric_fields
    
    def _extract_field_values(self, data: List[Dict], field: str, convert_to_string: bool = False) -> List[Any]:
        """Extract values for a specific field from all documents"""
        values = []
        
        for doc in data:
            value = self._get_nested_field_value(doc, field)
            
            if value is not None:
                if convert_to_string:
                    values.append(str(value))
                elif isinstance(value, (int, float)) and not isinstance(value, bool):
                    values.append(value)
                elif not convert_to_string:
                    # For non-numeric fields when not converting to string
                    values.append(str(value))
        
        return values
    
    def _get_nested_field_value(self, doc: Dict, field: str) -> Any:
        """Get field value, handling nested fields with dot notation"""
        if '.' not in field:
            return doc.get(field)
        
        # Handle nested field access (e.g., "categoryDetails.name")
        keys = field.split('.')
        value = doc
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
        
        return value
    
    def _calculate_value_distribution(self, data: List[Dict], group_by_field: str, value_field: str) -> Dict[str, float]:
        """Calculate distribution by summing values for each group"""
        distribution = defaultdict(float)
        
        for doc in data:
            group_value = self._get_nested_field_value(doc, group_by_field)
            aggregate_value = self._get_nested_field_value(doc, value_field)
            
            if group_value is not None and aggregate_value is not None:
                group_key = str(group_value)
                
                # Handle numeric values
                if isinstance(aggregate_value, (int, float)):
                    distribution[group_key] += aggregate_value
                else:
                    # If not numeric, just count occurrences
                    distribution[group_key] += 1
        
        return dict(distribution)


# Factory function
def create_analytics_engine() -> AnalyticsEngine:
    """Factory function to create analytics engine instance"""
    return AnalyticsEngine()


# Example usage
if __name__ == "__main__":
    # Test with different data structures
    
    # Test data 1: Raw documents from sitemaps
    raw_documents = [
        {"_id": "1", "name": "Page 1", "wordCount": 500, "categoryAttribute": "TOFU"},
        {"_id": "2", "name": "Page 2", "wordCount": 800, "categoryAttribute": "MOFU"},
        {"_id": "3", "name": "Page 3", "wordCount": 300, "categoryAttribute": "TOFU"},
        {"_id": "4", "name": "Page 4", "wordCount": 1200, "categoryAttribute": "BOFU"}
    ]
    
    # Test data 2: Aggregated results from query builder
    aggregated_results = [
        {"_id": "TOFU", "count": 15, "avg_word_count": 450},
        {"_id": "MOFU", "count": 8, "avg_word_count": 650},
        {"_id": "BOFU", "count": 5, "avg_word_count": 1100}
    ]
    
    analytics = create_analytics_engine()
    
    print("=== RAW DOCUMENTS ANALYSIS ===")
    # Auto-discover numeric fields and analyze
    summary = analytics.calculate_summary_stats(raw_documents)
    print("Summary Stats:", summary)
    
    # Analyze category distribution by count
    distribution = analytics.analyze_distribution(raw_documents, "categoryAttribute")
    print("Category Distribution:", distribution)
    
    # Analyze category distribution by word count
    word_distribution = analytics.analyze_distribution(raw_documents, "categoryAttribute", "wordCount")
    print("Word Count Distribution:", word_distribution)
    
    print("\n=== AGGREGATED RESULTS ANALYSIS ===")
    # Analyze pre-aggregated data
    agg_summary = analytics.calculate_summary_stats(aggregated_results)
    print("Aggregated Summary:", agg_summary)
    
    # Distribution of aggregated data
    agg_distribution = analytics.analyze_distribution(aggregated_results, "_id", "count")
    print("Aggregated Distribution:", agg_distribution)