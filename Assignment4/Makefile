# Makefile for MapReduce Page Rank project.

# Customize these paths for your environment.
# -----------------------------------------------------------
spark.root=/usr/local/spark
jar.name=spark-1.jar
jar.path=target/${jar.name}
job.name=org.apache.maven.App
local.input=/home/ajay/Documents/MR/Spark/Input
local.output=/home/ajay/Documents/MR/Spark/Output
# AWS EMR Execution
aws.emr.release=emr-5.2.1
aws.region=us-east-1
aws.bucket.name=mapreduceajay
aws.subnet.id=subnet-058ec328
aws.input=Input
aws.output=Output1
aws.log.dir=log
aws.num.nodes=10
aws.instance.type=m4.large
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package

# Removes local output directory.
clean-local-output:
	rm -rf ${local.output}*

# Runs standalone
# Make sure Hadoop  is set up (in /etc/hadoop files) for standalone operation (not pseudo-cluster).
# https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/SingleCluster.html#Standalone_Operation
alone: jar clean-local-output
	${spark.root}/bin/spark-submit --class ${job.name} --master local[*] ${jar.path} ${local.input} ${local.output}

# Upload data to S3 input dir.
upload-input-aws:
	aws s3 sync ${local.input} s3://${aws.bucket.name}/${aws.input}
	
# Delete S3 output dir.
delete-output-aws:
	aws s3 rm s3://${aws.bucket.name}/ --recursive --exclude "*" --include "${aws.output}*"

# Upload application to S3 bucket.
upload-app-aws:
	aws s3 cp ${jar.path} s3://${aws.bucket.name}

# Main EMR launch.
cloud: jar upload-input-aws upload-app-aws delete-output-aws
	aws emr create-cluster \
		--name "Page Rank" \
		--release-label ${aws.emr.release} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Spark \
	    --steps '[{"Name":"Spark Program", "Args":["--class", "${job.name}", "--master", "yarn", "--deploy-mode", "cluster", "s3://${aws.bucket.name}/${jar.name}", "s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}"],"Type":"Spark","ActionOnFailure":"TERMINATE_CLUSTER"}]' \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=${aws.subnet.id} \
		--configurations '[{"Classification":"spark", "Properties":{"maximizeResourceAllocation": "true"}}]' \
		--region ${aws.region} \
		--enable-debugging \
		--auto-terminate
