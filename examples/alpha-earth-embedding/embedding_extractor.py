#!/usr/bin/env python3
"""
AlphaEarth Embedding Extractor
Extracts Google Earth Engine satellite embeddings for genomic sample coordinates
"""

import ee
import pandas as pd
import numpy as np
import time
import argparse
from typing import Tuple, Optional, Dict, List
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AlphaEarthEmbeddingExtractor:
    """Extract satellite embeddings for genomic sample coordinates"""

    def __init__(self, batch_size: int = 100, min_year: Optional[int] = None):
        """
        Initialize the extractor

        Args:
            batch_size: Number of coordinates to process in each batch
            min_year: Minimum year threshold - samples older than this will be skipped (None = accept all years)
        """
        self.batch_size = batch_size
        self.min_year = min_year
        self.dataset = None
        self.embedding_bands = [f'A{i:02d}' for i in range(0, 64)]  # A00 to A63

        # Initialize Earth Engine
        try:
            ee.Initialize()
            self.dataset = ee.ImageCollection('GOOGLE/SATELLITE_EMBEDDING/V1/ANNUAL')
            logger.info("Earth Engine initialized successfully")
            if min_year:
                logger.info(f"Minimum year filter: {min_year}")
            else:
                logger.info("No year filtering applied - all years accepted")
        except Exception as e:
            logger.error(
                f"Failed to initialize Earth Engine: {e}. "
                "Authentication required — run 'earthengine authenticate' first."
            )
            raise

    def get_embeddings_for_point(self, lat: float, lon: float, year: int) -> Optional[Dict[str, float]]:
        """
        Extract embeddings for a single point (fallback method)

        Args:
            lat: Latitude in decimal degrees
            lon: Longitude in decimal degrees
            year: Year for which to get the embedding

        Returns:
            Dictionary with embedding values (A00-A63) or None if failed
        """
        try:
            # Create Earth Engine point
            point = ee.Geometry.Point([lon, lat])

            # Filter dataset for the sample's specific year and location
            image = (self.dataset
                    .filterDate(f'{year}-01-01', f'{year + 1}-01-01')
                    .filterBounds(point)
                    .first())

            # Sample the pixel at the point
            sample = image.sample(region=point, scale=10).first()

            # Get the dictionary of band values
            values = sample.toDictionary().getInfo()

            # Extract only the embedding bands
            embeddings = {}
            for band in self.embedding_bands:
                if band in values:
                    embeddings[band] = values[band]
                else:
                    logger.warning(f"Missing band {band} for point ({lat}, {lon}) in year {year}")
                    return None

            return embeddings

        except Exception as e:
            logger.warning(f"Failed to extract embeddings for point ({lat}, {lon}) in year {year}: {e}")
            return None

    def get_embeddings_bulk_by_year(self, coordinates: List[Tuple[int, float, float, int]], year: int) -> Dict[int, Optional[Dict[str, float]]]:
        """
        Extract embeddings for multiple points from a specific year using FeatureCollection (bulk method)

        Args:
            coordinates: List of (index, lat, lon, year) tuples for the same year
            year: The year to extract embeddings for

        Returns:
            Dictionary mapping index to embeddings (or None if failed)
        """
        # Create FeatureCollection of points
        features = []
        for idx, lat, lon, _ in coordinates:  # Ignore the year in tuple since we're processing by year
            point = ee.Geometry.Point([lon, lat])
            feature = ee.Feature(point, {'index': idx, 'lat': lat, 'lon': lon})
            features.append(feature)

        point_collection = ee.FeatureCollection(features)

        # Get the image for the specific year
        image = (self.dataset
                .filterDate(f'{year}-01-01', f'{year + 1}-01-01')
                .first())

        # Check if image is valid
        if image is None:
            logger.warning(f"No image found for year {year}")
            return {idx: None for idx, _, _, _ in coordinates}

        # Sample all points at once using sampleRegions
        sampled = image.sampleRegions(
            collection=point_collection,
            scale=10,
            geometries=True
        )

        # Only catch true infrastructure/API failures - not individual coordinate issues
        try:
            results_list = sampled.getInfo()
        except Exception as e:
            logger.warning(f"Earth Engine API call failed for year {year}: {e}")
            return {}  # True infrastructure failure - whole batch should fail

        # Process results and map back to original indices
        # Individual coordinate failures (no data, missing bands) are handled here normally
        results = {}
        for result in results_list['features']:
            properties = result['properties']
            idx = properties['index']

            # Extract embedding values
            embeddings = {}
            missing_bands = []

            for band in self.embedding_bands:
                if band in properties and properties[band] is not None:
                    embeddings[band] = properties[band]
                else:
                    missing_bands.append(band)

            if missing_bands:
                logger.debug(f"Missing bands {missing_bands} for point index {idx} in year {year}")
                results[idx] = None
            else:
                results[idx] = embeddings

        # Ensure all requested indices have results (even if None)
        for idx, _, _, _ in coordinates:
            if idx not in results:
                results[idx] = None

        return results

    def process_coordinates_batch(self, coordinates: List[Tuple[int, float, float, int]],
                                batch_num: int = 1, total_batches: int = 1) -> Dict[int, Optional[Dict[str, float]]]:
        """
        Process a batch of coordinates using bulk processing with fallback, grouped by year

        Args:
            coordinates: List of (index, lat, lon, year) tuples
            batch_num: Current batch number for progress tracking
            total_batches: Total number of batches for progress tracking

        Returns:
            Dictionary mapping index to embeddings (or None if failed)
        """
        start_time = time.time()
        results = {}

        # Group coordinates by year for efficient bulk processing
        coords_by_year = {}
        coords_without_year = []

        for coord in coordinates:
            idx, lat, lon, year = coord
            if year is not None and not pd.isna(year):
                year = int(year)
                if year not in coords_by_year:
                    coords_by_year[year] = []
                coords_by_year[year].append(coord)
            else:
                coords_without_year.append(coord)

        logger.info(f"Batch {batch_num}/{total_batches}: Processing {len(coordinates)} coordinates grouped by {len(coords_by_year)} years")

        # Process each year group with bulk processing
        for year, year_coords in coords_by_year.items():
            try:
                logger.info(f"  Processing {len(year_coords)} coordinates for year {year}...")

                # Try bulk processing for this year
                bulk_results = self.get_embeddings_bulk_by_year(year_coords, year)

                if bulk_results and len(bulk_results) > 0:
                    successful_bulk = sum(1 for v in bulk_results.values() if v is not None)
                    failed_bulk = len(year_coords) - successful_bulk

                    if successful_bulk > 0:
                        logger.info(f"    Bulk processing succeeded for {successful_bulk}/{len(year_coords)} coordinates")
                        results.update(bulk_results)

                        # Retry failed coordinates individually
                        if failed_bulk > 0:
                            failed_coords = [coord for coord in year_coords
                                           if coord[0] in bulk_results and bulk_results[coord[0]] is None]

                            if failed_coords:
                                logger.info(f"    Retrying {len(failed_coords)} failed coordinates individually...")
                                for idx, lat, lon, coord_year in failed_coords:
                                    try:
                                        embeddings = self.get_embeddings_for_point(lat, lon, coord_year)
                                        results[idx] = embeddings
                                        time.sleep(0.1)
                                    except Exception as e:
                                        logger.warning(f"Individual retry failed for coordinate {idx}: {e}")
                                        results[idx] = None
                    else:
                        raise Exception("Bulk processing returned no successful results")
                else:
                    raise Exception("Bulk processing returned empty results")

            except Exception as e:
                # Fall back to individual processing for this year
                logger.warning(f"    Bulk processing failed for year {year}, falling back to individual processing...")
                for idx, lat, lon, coord_year in year_coords:
                    try:
                        embeddings = self.get_embeddings_for_point(lat, lon, coord_year)
                        results[idx] = embeddings
                        time.sleep(0.1)
                    except Exception as point_error:
                        logger.warning(f"Failed to extract embeddings for coordinate {idx}: {point_error}")
                        results[idx] = None

        # Handle coordinates without year
        for idx, lat, lon, _ in coords_without_year:
            logger.warning(f"Skipping coordinate {idx} - no valid year available")
            results[idx] = None

        # Calculate batch statistics
        end_time = time.time()
        batch_duration = end_time - start_time
        successful_count = sum(1 for v in results.values() if v is not None)
        success_rate = (successful_count / len(coordinates) * 100) if coordinates else 0

        logger.info(f"Batch {batch_num}/{total_batches} complete: {successful_count}/{len(coordinates)} successful "
                   f"({success_rate:.1f}%) in {batch_duration:.1f}s")

        return results

    def process_tsv_file(self, tsv_path: str, output_path: str = None) -> pd.DataFrame:
        """
        Process TSV file with pre-cleaned coordinates and extract embeddings

        Expected input: TSV file processed by data_cleaner.py with columns:
        - cleaned_lat, cleaned_lon (from coordinate cleaning)
        - cleaned_year (from date cleaning)
        - coord_status, date_status (cleaning status indicators)

        Args:
            tsv_path: Path to input TSV file (should be pre-cleaned)
            output_path: Path for output CSV file (optional)

        Returns:
            DataFrame with original data plus embeddings
        """
        logger.info(f"Loading pre-cleaned TSV file: {tsv_path}")

        # Read the TSV file
        df = pd.read_csv(tsv_path, sep='\t')
        logger.info(f"Loaded {len(df)} rows from TSV file")

        # Verify required columns exist
        required_cols = ['cleaned_lat', 'cleaned_lon', 'cleaned_year']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}. " +
                           "Please run data_cleaner.py first to clean the input data.")

        # Initialize embedding columns
        for band in self.embedding_bands:
            df[band] = np.nan
        df['embedding_status'] = 'not_processed'

        # Filter and prepare coordinates for processing
        coordinates_to_process = []
        stats = {'valid': 0, 'invalid_coords': 0, 'invalid_year': 0, 'year_filtered': 0}

        for idx, row in df.iterrows():
            # Check if coordinates are valid (from data_cleaner.py)
            if (row.get('coord_status') == 'success' and
                pd.notna(row.get('cleaned_lat')) and pd.notna(row.get('cleaned_lon'))):

                lat, lon = row['cleaned_lat'], row['cleaned_lon']

                # Check if year is valid
                if (row.get('date_status') == 'success' and pd.notna(row.get('cleaned_year'))):
                    year = int(row['cleaned_year'])

                    # Apply min_year filtering first (if specified)
                    if self.min_year is not None and year < self.min_year:
                        df.at[idx, 'embedding_status'] = 'year_filtered'
                        stats['year_filtered'] += 1
                    else:
                        # Correct years before 2017 to 2017 (AlphaEarth satellite data availability)
                        processing_year = 2017 if year < 2017 else year

                        # Log year correction if applied
                        if year < 2017:
                            logger.debug(f"Row {idx}: Year corrected from {year} to {processing_year} (AlphaEarth data availability)")

                        coordinates_to_process.append((idx, lat, lon, processing_year))
                        stats['valid'] += 1
                else:
                    df.at[idx, 'embedding_status'] = 'invalid_year'
                    stats['invalid_year'] += 1
            else:
                df.at[idx, 'embedding_status'] = 'invalid_coords'
                stats['invalid_coords'] += 1

        logger.info(f"Processing statistics: {stats}")
        logger.info(f"Found {len(coordinates_to_process)} valid coordinates to process")

        if not coordinates_to_process:
            logger.warning("No valid coordinates found for processing!")
            return df

        # Process coordinates in batches
        total_processed = 0
        total_successful = 0
        total_batches = (len(coordinates_to_process) + self.batch_size - 1) // self.batch_size

        logger.info(f"Starting embedding extraction in {total_batches} batches (batch size: {self.batch_size})")

        for i in range(0, len(coordinates_to_process), self.batch_size):
            batch = coordinates_to_process[i:i + self.batch_size]
            batch_num = i // self.batch_size + 1

            batch_results = self.process_coordinates_batch(batch, batch_num, total_batches)

            # Update DataFrame with results
            for idx, embeddings in batch_results.items():
                total_processed += 1
                if embeddings:
                    # Set embedding values
                    for band, value in embeddings.items():
                        df.at[idx, band] = value
                    df.at[idx, 'embedding_status'] = 'success'
                    total_successful += 1
                else:
                    df.at[idx, 'embedding_status'] = 'extraction_failed'

            # Save intermediate results every 5 batches
            if batch_num % 5 == 0 and output_path:
                temp_path = output_path.replace('.tsv', f'_temp_batch_{batch_num}.tsv')
                df.to_csv(temp_path, index=False, delimiter='\t')
                logger.info(f"Saved intermediate results to {temp_path}")

        # Final statistics
        success_rate = (total_successful / total_processed * 100) if total_processed > 0 else 0

        logger.info("="*60)
        logger.info("EMBEDDING EXTRACTION COMPLETE")
        logger.info("="*60)
        logger.info(f"Total coordinates processed: {total_processed}")
        logger.info(f"Successful extractions: {total_successful}")
        logger.info(f"Success rate: {success_rate:.1f}%")
        logger.info("="*60)

        # Save final results
        if output_path:
            df.to_csv(output_path, index=False, delimiter='\t')
            logger.info(f"Results saved to {output_path}")

        return df

    def generate_summary_report(self, df: pd.DataFrame, output_dir: str = None):
        """
        Generate a summary report of the embedding extraction

        Args:
            df: DataFrame with embedding results
            output_dir: Directory to save report files
        """
        if output_dir:
            output_dir = Path(output_dir)
            output_dir.mkdir(exist_ok=True)

        # Embedding status summary
        status_counts = df['embedding_status'].value_counts()
        logger.info("Embedding extraction summary:")
        for status, count in status_counts.items():
            logger.info(f"  {status}: {count}")

        # Geographic coverage
        valid_coords = df[df['embedding_status'] == 'success']
        if len(valid_coords) > 0:
            lat_range = (valid_coords['cleaned_lat'].min(), valid_coords['cleaned_lat'].max())
            lon_range = (valid_coords['cleaned_lon'].min(), valid_coords['cleaned_lon'].max())
            logger.info(f"Geographic coverage - Lat: {lat_range}, Lon: {lon_range}")

        # Temporal coverage
        if 'cleaned_year' in df.columns:
            valid_years = df[df['cleaned_year'].notna()]
            if len(valid_years) > 0:
                year_range = (valid_years['cleaned_year'].min(), valid_years['cleaned_year'].max())
                logger.info(f"Temporal coverage - Year range: {year_range[0]} to {year_range[1]}")

        # Save detailed report
        if output_dir:
            report_path = output_dir / 'embedding_extraction_report.txt'
            with open(report_path, 'w') as f:
                f.write(f"AlphaEarth Embedding Extraction Report\n")
                f.write(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total Records: {len(df)}\n\n")

                f.write("Embedding Extraction Summary:\n")
                for status, count in status_counts.items():
                    f.write(f"  {status}: {count}\n")

                if len(valid_coords) > 0:
                    f.write(f"\nGeographic Coverage:\n")
                    f.write(f"  Latitude range: {lat_range[0]:.3f} to {lat_range[1]:.3f}\n")
                    f.write(f"  Longitude range: {lon_range[0]:.3f} to {lon_range[1]:.3f}\n")
                    f.write(f"  Total successful extractions: {len(valid_coords)}\n")

                if 'cleaned_year' in df.columns:
                    valid_years = df[df['cleaned_year'].notna()]
                    if len(valid_years) > 0:
                        year_range = (valid_years['cleaned_year'].min(), valid_years['cleaned_year'].max())
                        f.write(f"\nTemporal Coverage:\n")
                        f.write(f"  Year range: {year_range[0]} to {year_range[1]}\n")
                        f.write(f"  Total records with valid years: {len(valid_years)}\n")

            logger.info(f"Detailed report saved to {report_path}")


def main():
    """Main execution function with command-line argument support"""
    parser = argparse.ArgumentParser(description='AlphaEarth Embedding Extractor')
    parser.add_argument('--input', required=True, help='Input TSV file path (pre-cleaned by data_cleaner.py)')
    parser.add_argument('--output', help='Output CSV file path')
    parser.add_argument('--output-dir', help='Output directory for results and reports')
    parser.add_argument('--batch-size', type=int, default=100,
                        help='Batch size for processing coordinates (default: 100)')
    parser.add_argument('--min-year', type=int, default=None,
                        help='Minimum year threshold - samples older than this will be filtered out (optional)')

    args = parser.parse_args()

    try:
        # Initialize extractor
        logger.info("Initializing AlphaEarth Embedding Extractor...")
        extractor = AlphaEarthEmbeddingExtractor(
            batch_size=args.batch_size,
            min_year=args.min_year
        )

        # Process TSV file for embedding extraction
        logger.info("Starting embedding extraction...")
        results_df = extractor.process_tsv_file(args.input, args.output)

        # Generate summary report
        if args.output_dir:
            logger.info("Generating summary report...")
            extractor.generate_summary_report(results_df, args.output_dir)

        logger.info("Embedding extraction completed successfully!")

    except Exception as e:
        logger.error(f"Error during execution: {e}")
        raise


if __name__ == "__main__":
    main()
