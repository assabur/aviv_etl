from src.execution_context import ExecutionContext
from src.wrapper_pipeline import WrapperPipeline


def main():
    context = ExecutionContext(

                                #specification_file="config/silver/listing.yaml"
                                specification_file="config/gold/listing.yaml"
                               )

    config = context.get_configs()
    spark = context.get_spark()
    wrapper = WrapperPipeline(configs=config, spark=spark)
    wrapper.launch()


main()
