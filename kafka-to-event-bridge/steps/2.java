spring:
  application:
    name: kafka-to-eventbridge-payments-connector
  
# Configuración de Kafka
kafka:
  bootstrap-servers:
    - srhbmdlo21usb03.sys.mx.us.dev.corp:9092
  security:
    username: middle
    password: middleelkk_jaas
  consumer:
    group-id: HZ.PAYMENTSGDC.AVRO.CONSUMERTest13
    topic-pattern: SBNA.00002517.MIP_INS_HIST_EJ.MODIFY.AVRO

# Configuración de validación
validation:
  codesta2: "13"

# Configuración de AWS
aws:
  default: aws1
  datetime-format: yyyyMMdd'T'HHmmss'Z'
  algorithm: AWS4-X509-RSA-SHA256
  eventbridge:
    service: events
    aws1:
      host: events.us-east-1.amazonaws.com
      event-bus-name: dev-us-mb-sss
      region: us-east-1
    aws2:
      host: events.us-east-1.amazonaws.com
      event-bus-name: dev-us-sss-mb
      region: us-east-1

# Configuración de Redis
redis:
  host: redis-11999.redisesb.sys.mx.us.pre.corp
  port: 11999
  password: R3dis_EsB_Pr3