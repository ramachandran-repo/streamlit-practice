import os
import asyncio
import aiohttp
import logging
from typing import Any, Dict, List, Optional, AsyncIterator, Callable, TypeVar, cast

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

# ------------------------------------------------------------------------------
# Global Configuration
# ------------------------------------------------------------------------------
GRAPH_BASE_URL: str = "https://graph.microsoft.com/v1.0"
SP_BASE_URL: str = ""

# Endpoint templates for site details, subsites, lists, and permissions
SP_SITECOLLECTION_DETAILS_API_TEMPLATE: str = f"{SP_BASE_URL}/_api/sites/{{site_id}}"
SP_SUBSITES_API_TEMPLATE: str = f"{SP_BASE_URL}/sites/{{site_id}}/_api/web/webs"
SP_LISTS_API_TEMPLATE: str = f"{SP_BASE_URL}/sites/{{site_id}}/_api/web/lists"
SP_PERMISSIONS_API_TEMPLATE: str = f"{SP_BASE_URL}/sites/{{site_id}}/_api/web/roleassignments?$expand=Member,RoleDefinitionBindings"

# Output folders for enriched tables (Parquet)
PARQUET_BASE_PATH: str = "/mnt/parquet/sharepoint_metadata"

# Checkpoint file base path (will be suffixed with a unique key per stage)
CHECKPOINT_BASE_PATH: str = "/tmp/enrichment_checkpoint.json"

# ------------------------------------------------------------------------------
# Logging Setup
# ------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def log_event(level: str, message: str) -> None:
    if level.upper() == "INFO":
        logger.info(message)
    elif level.upper() == "WARNING":
        logger.warning(message)
    elif level.upper() == "ERROR":
        logger.error(message)
    else:
        logger.debug(message)

# ------------------------------------------------------------------------------
# Global Tokens & Headers (must be set in your environment)
# ------------------------------------------------------------------------------
if "graph_access_token" not in globals():
    raise ValueError("graph_access_token is required but not found.")
if "sp_access_token" not in globals():
    raise ValueError("sp_access_token is required but not found.")

GRAPH_HEADERS: Dict[str, str] = {"Authorization": f"Bearer {graph_access_token}"}
SP_HEADERS: Dict[str, str] = {"Authorization": f"Bearer {sp_access_token}"}

# ------------------------------------------------------------------------------
# Example Token Refresh Functions (stubs – replace with your logic)
# ------------------------------------------------------------------------------
def graph_token_generator() -> str:
    log_event("INFO", "Graph token refreshed (stub).")
    return "refreshed_graph_token"

def sp_token_generator() -> str:
    log_event("INFO", "SharePoint token refreshed (stub).")
    return "refreshed_sp_token"

# ------------------------------------------------------------------------------
# Async Retry Decorator
# ------------------------------------------------------------------------------
F = TypeVar("F", bound=Callable[..., Any])
def async_retry(func: F) -> F:
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        RETRY_COUNT = 5
        BACKOFF_FACTOR = 1
        for attempt in range(RETRY_COUNT):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                wait_time = BACKOFF_FACTOR * (2 ** attempt)
                log_event("WARNING", f"{func.__name__} attempt {attempt+1} failed: {e}. Retrying in {wait_time}s...")
                await asyncio.sleep(wait_time)
        log_event("ERROR", f"{func.__name__} failed after {RETRY_COUNT} attempts.")
        return None
    return cast(F, wrapper)

# ------------------------------------------------------------------------------
# APIClient: Centralized HTTP GET (with retry and token refresh)
# ------------------------------------------------------------------------------
class APIClient:
    def __init__(self, graph_headers: Dict[str, str], sp_headers: Dict[str, str]) -> None:
        self.graph_headers = graph_headers
        self.sp_headers = sp_headers

    @async_retry
    async def make_request(self, url: str, session: Optional[aiohttp.ClientSession] = None) -> Optional[Dict[str, Any]]:
        if url.startswith(GRAPH_BASE_URL):
            headers = self.graph_headers
        elif url.startswith(SP_BASE_URL):
            headers = self.sp_headers
        else:
            headers = self.graph_headers

        close_session = False
        if session is None:
            session = aiohttp.ClientSession(headers=headers)
            close_session = True

        try:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 401:
                    # Token expired; refresh token and trigger retry.
                    if url.startswith(GRAPH_BASE_URL):
                        new_token = graph_token_generator()
                        self.graph_headers = {"Authorization": f"Bearer {new_token}"}
                    elif url.startswith(SP_BASE_URL):
                        new_token = sp_token_generator()
                        self.sp_headers = {"Authorization": f"Bearer {new_token}"}
                    raise Exception("Token expired; retrying...")
                elif response.status in [429, 500, 502, 503, 504]:
                    raise Exception(f"HTTP {response.status} error; retrying...")
                else:
                    text = await response.text()
                    log_event("ERROR", f"Non-retryable HTTP {response.status}: {text}")
                    return None
        finally:
            if close_session:
                await session.close()

# ------------------------------------------------------------------------------
# Utility: Fetch Batches with Pagination (using APIClient)
# ------------------------------------------------------------------------------
async def fetch_batches(api_client: APIClient, url: str) -> AsyncIterator[List[Dict[str, Any]]]:
    while url:
        result = await api_client.make_request(url)
        if result is None:
            break
        records = result.get("value", [])
        yield records
        url = result.get("@odata.nextLink")

# ------------------------------------------------------------------------------
# MetadataProcessor: Enrichment Methods
# ------------------------------------------------------------------------------
class MetadataProcessor:
    @staticmethod
    def load_checkpoint(checkpoint_key: str) -> int:
        try:
            with open("/dbfs" + CHECKPOINT_BASE_PATH + f"_{checkpoint_key}", "r") as f:
                return int(f.read().strip())
        except FileNotFoundError:
            return 0
        except Exception as e:
            logger.error(f"Failed to load checkpoint '{checkpoint_key}': {e}")
            return 0

    @staticmethod
    def save_checkpoint(offset: int, checkpoint_key: str) -> None:
        try:
            with open("/dbfs" + CHECKPOINT_BASE_PATH + f"_{checkpoint_key}", "w") as f:
                f.write(str(offset))
            logger.info(f"Checkpoint '{checkpoint_key}' saved at offset: {offset}")
        except Exception as e:
            logger.error(f"Failed to save checkpoint '{checkpoint_key}': {e}")

    # --- Site Collection Details Enrichment ---
    @staticmethod
    async def fetch_site_collection_details_for_record(
        record: Dict[str, Any],
        api_client: APIClient,
        semaphore: asyncio.Semaphore,
        endpoint_template: str,
        key_column: str = "id"
    ) -> Dict[str, Any]:
        site_id = record.get(key_column)
        if not site_id:
            return record
        async with semaphore:
            url = endpoint_template.format(site_id=site_id)
            resp = await api_client.make_request(url)
            if resp:
                record["site_type"] = resp.get("WebTemplate", "Unknown")
                record["currentchangetoken"] = resp.get("CurrentChangeToken", None)
                record["hasuniqueroleassignments"] = resp.get("HasUniqueRoleAssignments", False)
        return record

    @staticmethod
    async def async_enrich_site_collection_details_to_temp_and_replace(
        api_client: APIClient,
        input_subfolder: str,
        output_subfolder: str,
        chunk_size: int,
        semaphore_limit: int,
        key_column: str = "id"
    ) -> Optional[DataFrame]:
        spark = SparkSession.builder.getOrCreate()
        input_path = f"{PARQUET_BASE_PATH}/{input_subfolder}"
        output_path = f"{PARQUET_BASE_PATH}/{output_subfolder}"

        df = await asyncio.to_thread(lambda: spark.read.parquet(input_path).select(key_column).dropna().distinct())
        df = df.withColumn("row_num", row_number().over(Window.orderBy(key_column)))
        total_rows = df.count()
        logger.info(f"[{output_subfolder}] Enriching {total_rows} site collections.")
        last_offset = MetadataProcessor.load_checkpoint(output_subfolder)

        for offset in range(last_offset, total_rows, chunk_size):
            logger.info(f"[{output_subfolder}] Processing rows {offset} to {offset+chunk_size}...")
            batch_df = await asyncio.to_thread(lambda: df.filter((col("row_num") > offset) & (col("row_num") <= offset+chunk_size)).drop("row_num"))
            rows = await asyncio.to_thread(lambda: batch_df.collect())
            rows_dicts = [row.asDict() for row in rows]
            sem_local = asyncio.Semaphore(semaphore_limit)
            tasks = [
                MetadataProcessor.fetch_site_collection_details_for_record(
                    record, api_client, sem_local, SP_SITECOLLECTION_DETAILS_API_TEMPLATE, key_column
                )
                for record in rows_dicts
            ]
            enriched_records = await asyncio.gather(*tasks)
            if enriched_records:
                enriched_df = await asyncio.to_thread(lambda: spark.createDataFrame(enriched_records))
                await asyncio.to_thread(lambda: enriched_df.coalesce(1).write.mode("append").parquet(output_path))
            MetadataProcessor.save_checkpoint(offset + chunk_size, output_subfolder)
        return await asyncio.to_thread(lambda: spark.read.parquet(output_path))

    # --- Nested Data Enrichment (Subsites / Lists) ---
    @staticmethod
    async def fetch_nested_data_for_record(
        record: Dict[str, Any],
        api_client: APIClient,
        semaphore: asyncio.Semaphore,
        endpoint_template: str,
        current_level: int,
        key_column: str = "id"
    ) -> List[Dict[str, Any]]:
        results = []
        site_id = record.get(key_column)
        if not site_id:
            return results
        async with semaphore:
            url = endpoint_template.format(site_id=site_id)
            async for page in fetch_batches(api_client, url):
                for item in page:
                    item["nest_level"] = current_level + 1
                    item["parent_site_id"] = site_id
                    results.append(item)
        return results

    @staticmethod
    async def fetch_nested_for_records(
        records: List[Dict[str, Any]],
        api_client: APIClient,
        semaphore_limit: int,
        endpoint_template: str,
        current_level: int,
        key_column: str = "id"
    ) -> List[Dict[str, Any]]:
        sem = asyncio.Semaphore(semaphore_limit)
        tasks = [
            MetadataProcessor.fetch_nested_data_for_record(record, api_client, sem, endpoint_template, current_level, key_column)
            for record in records
        ]
        nested_records_lists = await asyncio.gather(*tasks)
        results = []
        for nr in nested_records_lists:
            results.extend(nr)
        return results

    @staticmethod
    async def async_enrich_nested_level_to_temp_and_append(
        api_client: APIClient,
        input_subfolder: str,
        output_subfolder: str,
        chunk_size: int,
        semaphore_limit: int,
        target_nest_level: int,
        endpoint_template: str,
        key_column: str = "id",
        nest_level_column: str = "nest_level"
    ) -> Optional[DataFrame]:
        spark = SparkSession.builder.getOrCreate()
        input_path = f"{PARQUET_BASE_PATH}/{input_subfolder}"
        output_path = f"{PARQUET_BASE_PATH}/{output_subfolder}"

        df = await asyncio.to_thread(lambda: spark.read.parquet(input_path))
        if nest_level_column in df.columns:
            df = df.filter(col(nest_level_column) == target_nest_level)
        else:
            log_event("WARNING", f"'{nest_level_column}' not found in {input_subfolder}. Processing all rows.")
        df = df.select(key_column).dropna().distinct()
        df = df.withColumn("row_num", row_number().over(Window.orderBy(key_column)))
        total_rows = df.count()
        logger.info(f"[{output_subfolder}] Found {total_rows} records at nest_level={target_nest_level} to enrich.")
        last_offset = MetadataProcessor.load_checkpoint(output_subfolder)

        for offset in range(last_offset, total_rows, chunk_size):
            logger.info(f"[{output_subfolder}] Processing rows {offset} to {offset+chunk_size}...")
            batch_df = await asyncio.to_thread(lambda: df.filter((col("row_num") > offset) & (col("row_num") <= offset+chunk_size)).drop("row_num"))
            rows = await asyncio.to_thread(lambda: batch_df.collect())
            rows_dicts = [r.asDict() for r in rows]
            enriched_results = await MetadataProcessor.fetch_nested_for_records(
                records=rows_dicts,
                api_client=api_client,
                semaphore_limit=semaphore_limit,
                endpoint_template=endpoint_template,
                current_level=target_nest_level,
                key_column=key_column
            )
            if enriched_results:
                enriched_df = await asyncio.to_thread(lambda: spark.createDataFrame(enriched_results))
                await asyncio.to_thread(lambda: enriched_df.coalesce(1).write.mode("append").parquet(output_path))
            MetadataProcessor.save_checkpoint(offset + chunk_size, output_subfolder)
        return await asyncio.to_thread(lambda: spark.read.parquet(output_path))

    # --- Permission Data Enrichment ---
    @staticmethod
    async def fetch_permissions_for_record(
        record: Dict[str, Any],
        api_client: APIClient,
        semaphore: asyncio.Semaphore,
        endpoint_template: str,
        key_column: str = "id"
    ) -> Dict[str, Any]:
        site_id = record.get(key_column)
        if not site_id:
            return record
        async with semaphore:
            url = endpoint_template.format(site_id=site_id)
            resp = await api_client.make_request(url)
            if resp:
                # Store the list of permission assignments.
                record["permissions"] = resp.get("value", [])
        return record

    @staticmethod
    async def async_enrich_permissions_to_temp_and_replace(
        api_client: APIClient,
        input_subfolder: str,
        output_subfolder: str,
        chunk_size: int,
        semaphore_limit: int,
        key_column: str = "id",
        nest_level_column: str = "nest_level"
    ) -> Optional[DataFrame]:
        spark = SparkSession.builder.getOrCreate()
        input_path = f"{PARQUET_BASE_PATH}/{input_subfolder}"
        output_path = f"{PARQUET_BASE_PATH}/{output_subfolder}"

        df = await asyncio.to_thread(lambda: spark.read.parquet(input_path))
        # Process all records regardless of nest level; you may filter if desired.
        df = df.select(key_column).dropna().distinct()
        df = df.withColumn("row_num", row_number().over(Window.orderBy(key_column)))
        total_rows = df.count()
        logger.info(f"[{output_subfolder}] Enriching permission data for {total_rows} records from {input_subfolder}.")
        last_offset = MetadataProcessor.load_checkpoint(output_subfolder)

        for offset in range(last_offset, total_rows, chunk_size):
            logger.info(f"[{output_subfolder}] Processing rows {offset} to {offset+chunk_size}...")
            batch_df = await asyncio.to_thread(lambda: df.filter((col("row_num") > offset) & (col("row_num") <= offset+chunk_size)).drop("row_num"))
            rows = await asyncio.to_thread(lambda: batch_df.collect())
            rows_dicts = [row.asDict() for row in rows]
            sem_local = asyncio.Semaphore(semaphore_limit)
            tasks = [
                MetadataProcessor.fetch_permissions_for_record(
                    record, api_client, sem_local, SP_PERMISSIONS_API_TEMPLATE, key_column
                )
                for record in rows_dicts
            ]
            enriched_records = await asyncio.gather(*tasks)
            if enriched_records:
                enriched_df = await asyncio.to_thread(lambda: spark.createDataFrame(enriched_records))
                await asyncio.to_thread(lambda: enriched_df.coalesce(1).write.mode("append").parquet(output_path))
            MetadataProcessor.save_checkpoint(offset + chunk_size, output_subfolder)
        return await asyncio.to_thread(lambda: spark.read.parquet(output_path))

# ------------------------------------------------------------------------------
# run_ingestion: Centralized Async Orchestration
# ------------------------------------------------------------------------------
async def run_ingestion(max_depth: Optional[int] = None):
    spark = SparkSession.builder.getOrCreate()
    api_client = APIClient(graph_headers=GRAPH_HEADERS, sp_headers=SP_HEADERS)

    # Step 1: Enrich Site Collection Details
    task_site_details = asyncio.create_task(
        MetadataProcessor.async_enrich_site_collection_details_to_temp_and_replace(
            api_client=api_client,
            input_subfolder="site_collections",            # Raw site collections extracted from Graph
            output_subfolder="site_collections_enriched_details",
            chunk_size=500,
            semaphore_limit=20,
            key_column="id"
        )
    )
    site_details_df = await task_site_details
    if site_details_df:
        log_event("INFO", f"Site Collection details enriched: {site_details_df.count()} records")
    else:
        log_event("ERROR", "Site Collection details enrichment failed.")
        return

    # Step 1a: Enrich Permissions for Site Collections
    task_permissions_site = asyncio.create_task(
        MetadataProcessor.async_enrich_permissions_to_temp_and_replace(
            api_client=api_client,
            input_subfolder="site_collections_enriched_details",
            output_subfolder="site_collections_permissions",
            chunk_size=500,
            semaphore_limit=20,
            key_column="id",
            nest_level_column="nest_level"  # May not exist for site collections; processed regardless.
        )
    )
    permissions_site_df = await task_permissions_site
    if permissions_site_df:
        log_event("INFO", f"Permissions enriched for site collections: {permissions_site_df.count()} records")
    else:
        log_event("ERROR", "Permissions enrichment for site collections failed.")

    # Step 2: Iteratively Extract Subsites Level-by-Level
    current_depth = 0  # Site collections are treated as nest_level 0
    while True:
        if max_depth is not None and current_depth >= max_depth:
            log_event("INFO", f"Reached max subsites extraction depth of {max_depth}.")
            break
        log_event("INFO", f"Extracting subsites for nest_level {current_depth} ...")
        next_level_df = await MetadataProcessor.async_enrich_nested_level_to_temp_and_append(
            api_client=api_client,
            input_subfolder="subsites_enriched" if current_depth > 0 else "site_collections_enriched_details",
            output_subfolder="subsites_enriched",
            chunk_size=500,
            semaphore_limit=20,
            target_nest_level=current_depth,
            endpoint_template=SP_SUBSITES_API_TEMPLATE,
            key_column="id",
            nest_level_column="nest_level"
        )
        if next_level_df is None or next_level_df.count() == 0:
            log_event("INFO", f"No new subsites found at nest_level {current_depth}.")
            break
        else:
            log_event("INFO", f"Extracted {next_level_df.count()} subsites for nest_level {current_depth+1}.")
            current_depth += 1

    # Step 2a: Enrich Permissions for Subsites
    task_permissions_subsites = asyncio.create_task(
        MetadataProcessor.async_enrich_permissions_to_temp_and_replace(
            api_client=api_client,
            input_subfolder="subsites_enriched",
            output_subfolder="subsites_permissions",
            chunk_size=500,
            semaphore_limit=20,
            key_column="id",
            nest_level_column="nest_level"
        )
    )
    permissions_subsites_df = await task_permissions_subsites
    if permissions_subsites_df:
        log_event("INFO", f"Permissions enriched for subsites: {permissions_subsites_df.count()} records")
    else:
        log_event("ERROR", "Permissions enrichment for subsites failed.")

    # Step 3: Iteratively Extract Lists from All Levels
    current_list_depth = 0
    while True:
        if max_depth is not None and current_list_depth >= max_depth:
            log_event("INFO", f"Reached max lists extraction depth of {max_depth}.")
            break
        log_event("INFO", f"Extracting lists for sites with nest_level {current_list_depth} ...")
        next_lists_df = await MetadataProcessor.async_enrich_nested_level_to_temp_and_append(
            api_client=api_client,
            input_subfolder="lists_enriched" if current_list_depth > 0 else "site_collections_enriched_details",
            output_subfolder="lists_enriched",
            chunk_size=500,
            semaphore_limit=20,
            target_nest_level=current_list_depth,
            endpoint_template=SP_LISTS_API_TEMPLATE,
            key_column="id",
            nest_level_column="nest_level"
        )
        if next_lists_df is None or next_lists_df.count() == 0:
            log_event("INFO", f"No additional lists found at nest_level {current_list_depth}.")
            break
        else:
            log_event("INFO", f"Extracted {next_lists_df.count()} lists for nest_level {current_list_depth+1}.")
            current_list_depth += 1

    # Step 3a: Enrich Permissions for Lists
    task_permissions_lists = asyncio.create_task(
        MetadataProcessor.async_enrich_permissions_to_temp_and_replace(
            api_client=api_client,
            input_subfolder="lists_enriched",
            output_subfolder="lists_permissions",
            chunk_size=500,
            semaphore_limit=20,
            key_column="id",
            nest_level_column="nest_level"
        )
    )
    permissions_lists_df = await task_permissions_lists
    if permissions_lists_df:
        log_event("INFO", f"Permissions enriched for lists: {permissions_lists_df.count()} records")
    else:
        log_event("ERROR", "Permissions enrichment for lists failed.")

    # Final Verification: Log final schemas and counts for subsites and lists permissions
    try:
        final_subsites = spark.read.parquet(f"{PARQUET_BASE_PATH}/subsites_permissions")
        log_event("INFO", f"Final Subsites Permissions Schema: {final_subsites.schema}")
        log_event("INFO", f"Final Subsites Permissions Count: {final_subsites.count()}")
    except Exception as e:
        log_event("ERROR", f"Failed to read final subsites permissions: {e}")

    try:
        final_lists = spark.read.parquet(f"{PARQUET_BASE_PATH}/lists_permissions")
        log_event("INFO", f"Final Lists Permissions Schema: {final_lists.schema}")
        log_event("INFO", f"Final Lists Permissions Count: {final_lists.count()}")
    except Exception as e:
        log_event("ERROR", f"Failed to read final lists permissions: {e}")

    spark.stop()

# ------------------------------------------------------------------------------
# Main Entry Point
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    # Set max_depth for testing (e.g., 2). Set to None in production for unlimited extraction.
    asyncio.run(run_ingestion(max_depth=2))
