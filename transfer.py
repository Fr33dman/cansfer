from typing import Dict, Any
from base import SourceAbstract, DestinationAbstract
from ddl import DDL
from exc import CreateTableException, ReadDDLException, ReadDataException, WriteDataException


def transfer(
        ctx: Dict[str, Any],
        src: SourceAbstract,
        dst: DestinationAbstract,
        milestone_chunk_index: int,
        save_progress: callable = None,
) -> int:

    try:
        # Read DDL of source table
        ddl = src.read_ddl(
            ctx=ctx,
        )

        # Read data
        _chunk = src.read(ctx)  # Read pure data from source table
        _chunk = src.post_process_data(ctx, ddl, _chunk)  # Replace specific for source database dtypes by common

        # Create destination table in database (clickhouse)
        dst.create_table(
            ctx=ctx,
            ddl=ddl,
        )

        # Write data
        _chunk = dst.pre_process_data(ctx, ddl, _chunk)  # Prepare columns for inserting
        dst.write(ctx, _chunk)  # Write data into destination table

    except ReadDDLException:
        return 1
    except ReadDataException:
        return 1
    except CreateTableException:
        return 1
    except WriteDataException as e:
        return 1
    finally:
        if save_progress:
            save_progress(ctx)  # Save progress if write fail/success

    return 0
