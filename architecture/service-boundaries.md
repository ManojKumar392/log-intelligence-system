Ingestion Service

    Does:

        Accept logs via REST API

        Validate request format

        Push raw logs into Kafka

    Does NOT:

        Clean logs

        Analyze logs

        Save logs to DB

Processing Service

    Does:

        Read logs from Kafka

        Extract timestamp, service, level, message

        Save raw logs to S3

        Publish cleaned logs to another Kafka topic

    Does NOT:

        Run ML

        Trigger alerts

        Talk to users

ML Service

    Does:

        Read cleaned logs

        Perform anomaly detection

        Save anomalies to DB

    Does NOT:

        Send alerts

        Talk to frontend

        Store raw logs

Alert Engine

    Does:

        Monitor anomaly events

        Send notifications (email/webhook/websocket)

        Store alert history

    Does NOT:

        Analyze logs

        Process logs

API Gateway

    Does:

        Expose REST endpoints for dashboard

        Aggregate data from DB

    Does NOT:

        Touch Kafka

        Run ML

        Receive logs

Dashboard

    Does:

        Visualize logs/anomalies/alerts

        Provide filters and charts

    Does NOT:

        Perform backend work

        Store data