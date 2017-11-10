# Spark on Yarn with FPGA

Spark will support to specify FPGA resource when user submit application to Yarn.

## Building Spark

FPGA on Yarn is required here, please clone and install it first.

	git clone https://github.com/intel-hadoop/hadoop.git -b YARN-5983
	mvn clean install -DskipTests -Pdist,native

Build spark on yarn

	./build/mvn -Pyarn -Phadoop-3.1 -Dhadoop.version=3.1.0-SNAPSHOT -DskipTests clean package

## Usage

	$SPARK_HOME/spark-submit --class "CLASS_NAME" \
	--master yarn \
	--conf spark.executor.fpga.type=DCP \
	--conf spark.executor.fpga.ip=AFU_ID_1:COUNT \
	--conf spark.executor.fpga.ip=AFU_ID_2:COUNT \
	your_spark_application.jar
