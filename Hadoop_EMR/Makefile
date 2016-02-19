format: 
	hdfs namenode -format

hstart:
	start-dfs.sh
	start-yarn.sh
	mr-jobhistory-daemon.sh start historyserver


hstop:
	mr-jobhistory-daemon.sh stop historyserver 
	stop-yarn.sh
	stop-dfs.sh

abhijeet:
	hadoop fs -mkdir -p /user/abhijeet
	hadoop fs -mkdir -p /user/abhijeet/input

pseudo:
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-2.7.2.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.7.2.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.2.jar:  CarrierCount.java
	jar cf cc.jar *.class	
	hadoop fs -rm -r output
	hadoop jar cc.jar CarrierCount /user/abhijeet/input /user/abhijeet/output
	hadoop fs -get /user/abhijeet/output
	Rscript -e "rmarkdown::render('Assignment2_Report.Rmd')"

cloud:
	javac -cp /usr/local/hadoop/share/hadoop/mapreduce/lib/hadoop-annotations-2.7.2.jar:/usr/local/hadoop/share/hadoop/common/hadoop-common-2.7.2.jar:/usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.7.2.jar:  CarrierCount.java
	jar cf cc.jar *.class
	aws s3 cp cc.jar s3://<<ENTER BUCKET NAME>>
	aws s3 cp ./input s3://<<Enter Bucket Name>>/input --recursive
	aws emr create-cluster 
		--name "CLI-Cluster" 
		--release-label emr-4.3.0 
		--instance-groups InstanceGroupType=Master,InstanceCount=1,InstanceType=c1.medium \
						  InstanceGroupType=CORE,InstanceCount=2,InstanceType=c1.medium 
		--steps Type=CUSTOM_JAR,\
				Name="CusJarStep",\
				ActionOnFailure=CONTINUE,\
				Jar=s3://<<ENTER BUCKET NAME>>/cc.jar,\
				Args=[CarrierCount,s3n://<<ENTER BUCKET NAME>>/input/,s3n://<<ENTER BUCKET NAME>>/output/]
		--auto-terminate \
		--log-uri s3://<<ENTER BUCKET NAME>>/logs \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,AvailabilityZone=us-east-1a\
		--enable-debugging