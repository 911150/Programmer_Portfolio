from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler


# TODO: allow passthrough/dynamic model parameters
def build_pipeline(model_features, fit=False):
    # gradient boosted tree regressor
    gbt = GBTRegressor(featuresCol="features",
                       labelCol="fare_amount",
                       maxDepth=5,
                       maxIter=5,
                       seed=0)
    # vector assembler
    va = get_vector_assembler(model_features)

    if fit:
        pipeline = PipelineModel(stages=[va, gbt])
    else:
        pipeline = Pipeline(stages=[va, gbt])

    return pipeline


def build_pipeline_rf(model_features, fit=False):
    # random forest regressor
    rfr = RandomForestRegressor(featuresCol="features",
                                labelCol="fare_amount",
                                maxDepth=5,
                                seed=0)
    # vector assembler
    va = get_vector_assembler(model_features)

    if fit:
        pipeline = PipelineModel(stages=[va, rfr])
    else:
        pipeline = Pipeline(stages=[va, rfr])

    return pipeline


def get_evaluator(metric):
    evaluator = RegressionEvaluator(labelCol="fare_amount",
                                    predictionCol="prediction",
                                    metricName=metric)
    return evaluator


def get_vector_assembler(model_features):
    va = VectorAssembler(inputCols=model_features,
                         outputCol="features",
                         handleInvalid='skip')
    return va


def train_model(sdf, pipeline):
    model = pipeline.fit(sdf)
    return model
