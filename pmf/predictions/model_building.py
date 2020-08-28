from pyspark.ml.regression import LinearRegression, DecisionTreeRegressor, RandomForestRegressor, GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml import Pipeline

from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler, PCA


def build_and_save_model(df,config):
    #  Feature engineering
    categorical_cols = config["categorical_cols"]
    numerical_cols = config["numerical_cols"]
    indexed_cols =  [c+'_index' for c in categorical_cols]
    encoded_cols = [c+'_dummy' for c in categorical_cols]
    
    indexer = StringIndexer(inputCols = categorical_cols, outputCols = indexed_cols, handleInvalid = 'keep') 
    onehotencoder = OneHotEncoder(inputCols = indexed_cols, outputCols = encoded_cols)
    assembler = VectorAssembler(inputCols = encoded_cols, outputCol = 'features') 
    standardscaler = StandardScaler(inputCol = 'features', outputCol = 'normalized_features', withMean = True)
    pca = PCA(inputCol = 'normalized_features', outputCol = 'pca_features')
    
    #Tune Model
    
    model_selector = 'RandomForest'
    
    if model_selector == 'ElasticNet':
        regressor = LinearRegression(featuresCol = 'pca_features')
        grid = ParamGridBuilder() \
            .addGrid(pca.k,[2,5,10]) \
            .addGrid(regressor.maxIter,[10]) \
            .addGrid(regressor.regParam,[0.1,0.3]) \
            .addGrid(regressor.elasticNetParam,[0.0,0.3,0.5,0.8,1.0]) \
            .build()
        stages = [indexer,onehotencoder,assembler,standardscaler,pca,regressor]
    
    if model_selector == 'DecisionTree':
        regressor = DecisionTreeRegressor(featuresCol = 'pca_features') 
        grid = ParamGridBuilder() \
            .addGrid(pca.k,[2,10,20]) \
            .addGrid(regressor.maxDepth,[5,10,20]) \
            .addGrid(regressor.maxBins,[10]) \
            .build()
        stages = [indexer,onehotencoder,assembler,standardscaler,pca,regressor]
    
    if model_selector == 'RandomForest':
        regressor = RandomForestRegressor(featuresCol = 'features') 
        grid = ParamGridBuilder() \
            .addGrid(regressor.maxDepth,[2,5,10]) \
            .addGrid(regressor.maxBins,[10]) \
            .addGrid(regressor.numTrees,[20]) \
            .build()
        stages = [indexer,onehotencoder,assembler,regressor]
    
    if model_selector == "GradientBoosting":
        regressor = GBTRegressor(featuresCol = 'features')
        grid = ParamGridBuilder() \
            .addGrid(regressor.maxDepth,[2,5]) \
            .addGrid(regressor.maxBins,[8]) \
            .addGrid(regressor.maxIter,[10]) \
            .build()
        stages = [indexer,onehotencoder,assembler,regressor]
    
    pipeline = Pipeline(stages = stages)
    
    cv = CrossValidator(
        estimator = pipeline,
        estimatorParamMaps = grid,
        evaluator = RegressionEvaluator(metricName = 'mae'),
        numFolds = 10,
        parallelism = 3,
        seed = 123)
    
    model = cv.fit(df)
    
    print('mean absolute error: %s' % model.avgMetrics)
    print(model.bestModel.stages[-1])
    model.write().overwrite().save(config["model_path"])
    
