from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler

def build_pipeline(model_features, model=False):

    va = VectorAssembler(inputCols=model_features, outputCol="features", handleInvalid='skip')
    gbt = GBTRegressor(featuresCol="features", labelCol="fare_amount", maxDepth=5, maxIter=5, seed=0)

    if model == True:
        pipeline = PipelineModel(stages=[va, gbt])
    else:
        pipeline = Pipeline(stages=[va, gbt])

    return pipeline


def build_pipeline_rf(model_features, model=False):

    va = VectorAssembler(inputCols=model_features, outputCol="features", handleInvalid='skip')
    gbt = RandomForestRegressor(featuresCol="features", labelCol="fare_amount", maxDepth=5, seed=0)

    if model == True:
        pipeline = PipelineModel(stages=[va, gbt])
    else:
        pipeline = Pipeline(stages=[va, gbt])

    return pipeline




def get_evaluator(metric):
    evaluator = RegressionEvaluator(labelCol="fare_amount", predictionCol="prediction", metricName=metric)
    return evaluator


# def fit_model(sdf, pipeline):
#     return pipeline.fit(sdf)


def  train_model(sdf, pipeline):
    model = pipeline.fit(sdf)
    return model



#%%
