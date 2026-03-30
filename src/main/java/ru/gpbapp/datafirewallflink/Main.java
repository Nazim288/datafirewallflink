package ru.gpbapp.datafirewallflink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.gpbapp.datafirewallflink.config.JobConfig;
import ru.gpbapp.datafirewallflink.kafka.CacheUpdateEvent;
import ru.gpbapp.datafirewallflink.kafka.CacheUpdateEventDeserializationSchema;
import ru.gpbapp.datafirewallflink.mq.MqRecord;
import ru.gpbapp.datafirewallflink.mq.MqReply;
import ru.gpbapp.datafirewallflink.mq.MqSink;
import ru.gpbapp.datafirewallflink.mq.MqSource;
import ru.gpbapp.datafirewallflink.services.MqWithRulesReloadBroadcastProcessFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    // Тестовый JSON для режима --use.mq=false
    private static final String TEST_JSON = """
{
  "dfw_hostname": "mkd-d32asl-wrk",
  "dfw_user_login": "a_mkd_e_mq",
  "dfw_readed_from_mq_dttm": "2025-09-01T12:02:50.4124228Z",
  "dfw_action_dttm": "2025-09-01T12:02:50.4204910Z",
  "dfw_action_type": "QUERY",
  "dfw_created_dttm": "2025-09-01T12:02:50.394Z",
  "dfw_dataset_code": "Дашборд.УС ЛИК",
  "data": {
    "homeAddress": {
      "mapping.block": "АДРЕС.Строение",
      "mapping.building": "none",
      "mapping.areaType": "none",
      "regionCode": "24",
      "postalCode": "660043",
      "mapping.area": "АДРЕС.Район",
      "mapping.houseNumber": "none",
      "regionType": "край",
      "mapping.regionType": "none",
      "area": null,
      "dataset_code": "УС.ЛиК.Адрес проживания",
      "flat": "48",
      "block": null,
      "regionName": "Красноярский",
      "mapping.postalCode": "АДРЕС.Почтовый индекс",
      "district": "Сибирский",
      "settlement": null,
      "mapping.district": "none",
      "countryCode": "643",
      "mapping.regionName": "АДРЕС.Наименование региона",
      "areaType": null,
      "building": null,
      "mapping.countryCode": "АДРЕС.Код страны",
      "mapping.street": "АДРЕС.Улица",
      "houseNumber": "67",
      "mapping.regionCode": "none",
      "street": "Березина",
      "city": "Красноярск",
      "mapping.countryName": "АДРЕС.Страна",
      "mapping.flat": "none",
      "countryName": "Россия",
      "mapping.settlement": "АДРЕС.Населенный пункт",
      "mapping.city": "АДРЕС.Наименование города"
    },
    "documents": {
      "mapping.clientSnils": "ОСНОВНЫЕ СВЕДЕНИЯ.СНИЛС",
      "clientInn": null,
      "dataset_code": "УС.ЛиК .Документы клиента",
      "clientIdCard": [
        {
          "issueAuthority": "ТП ОУФМС РОССИИ ПО МОСКОВСКОЙ ОБЛ.В ДМИТРОВСКОМ Р-НЕ В ПОС. ИШКЕ",
          "mapping.issueDate": "ДУЛ.Паспорт РФ.Дата выдачи",
          "mapping.primary": "ДУЛ.Паспорт РФ.Признак основного",
          "mapping.issueAuthority": "ДУЛ.Паспорт РФ.Кем выдан",
          "primary": true,
          "number": "518273",
          "expiryDate": null,
          "mapping.series": "ДУЛ.Паспорт РФ.Серия",
          "mapping.number": "ДУЛ.Паспорт РФ.Номер",
          "mapping.expiryDate": "none",
          "mapping.departmentCode": "ДУЛ.Паспорт РФ.Код подразделения",
          "mapping.type": "ДУЛ.Паспорт РФ.Тип ДУЛ",
          "type": "PASSPORT_RU",
          "issueDate": "2021-12-29",
          "elemId": "PASSPORT_RU",
          "departmentCode": "500-020",
          "series": "4621"
        }
      ],
      "clientSnils": "344-889-028 21",
      "mapping.clientInn": "ИНН.Номер свидетельства"
    },
    "registrationAddress": {
      "mapping.block": "АДРЕС.Строение",
      "mapping.building": "none",
      "mapping.areaType": "none",
      "regionCode": "24",
      "postalCode": "660043",
      "mapping.area": "АДРЕС.Район",
      "mapping.houseNumber": "none",
      "regionType": "край",
      "mapping.regionType": "none",
      "area": null,
      "dataset_code": "УС.ЛиК.Адрес регистрации",
      "flat": "48",
      "block": null,
      "regionName": "Красноярский",
      "mapping.postalCode": "АДРЕС.Почтовый индекс",
      "district": "Сибирский",
      "settlement": null,
      "mapping.district": "none",
      "countryCode": "643",
      "mapping.regionName": "АДРЕС.Наименование региона",
      "areaType": null,
      "building": null,
      "mapping.countryCode": "АДРЕС.Код страны",
      "mapping.street": "АДРЕС.Улица",
      "houseNumber": "67",
      "mapping.regionCode": "none",
      "street": "Березина",
      "city": "Красноярск",
      "mapping.countryName": "АДРЕС.Страна",
      "mapping.flat": "none",
      "countryName": "Россия",
      "mapping.settlement": "АДРЕС.Населенный пункт",
      "mapping.city": "АДРЕС.Наименование города"
    },
    "contactInfo": {
      "mobilePhone": "79859971243",
      "dataset_code": "УС.ЛИК.Контакты клиента",
      "mapping.emailValue": "КОНТАКТ.Почта.Электронный адрес (email)",
      "mapping.mobilePhone": "КОНТАКТ.Телефон.Номер телефона",
      "emailValue": "makogonenkoadel66@gmail.com"
    },
    "baseInfo": {
      "birthCountry": "РОССИЙСКАЯ ФЕДЕРАЦИЯ",
      "birthdate": "1976-10-30",
      "mapping.surname": "ОСНОВНЫЕ СВЕДЕНИЯ.Фамилия",
      "isPatronymicLack": null,
      "gender": "FEMALE",
      "dataset_code": "УС.ЛиК.Данные клиента",
      "mapping.name": "ОСНОВНЫЕ СВЕДЕНИЯ.Имя",
      "birthPlace": "Алтай",
      "mapping.fullName": "ОСНОВНЫЕ СВЕДЕНИЯ.ФИО одной строкой",
      "surname": "Макогоненко",
      "mapping.birthPlace": "ОСНОВНЫЕ СВЕДЕНИЯ.Место рождения",
      "mapping.isPatronymicLack": "ОСНОВНЫЕ СВЕДЕНИЯ.Признак отсутствия отчества",
      "name": "Адель",
      "fullName": "Макогоненко Адель Фроловна",
      "mapping.patronymic": "ОСНОВНЫЕ СВЕДЕНИЯ.Отчество",
      "mapping.birthdate": "ОСНОВНЫЕ СВЕДЕНИЯ.Дата рождения",
      "patronymic": "Фроловна",
      "citizenship": ["РОССИЙСКАЯ ФЕДЕРАЦИЯ"],
      "mapping.citizenship": "ГРАЖДАНСТВО.Страна",
      "mapping.birthCountry": "none",
      "mapping.gender": "ОСНОВНЫЕ СВЕДЕНИЯ.Пол"
    }
  },
  "dfw_query_id": "ID:05510000000000002b7a0bcb516145a0b7ee76c93dd53ece"
}
""";

    public static void main(String[] args) throws Exception {

        String[] normalized = normalizeArgs(args);
        ParameterTool pt = ParameterTool.fromArgs(normalized);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(pt);

        int p = pt.getInt("parallelism", 1);
        env.setParallelism(p);

        JobConfig cfg = JobConfig.fromArgs(normalized);

        String mqUser = cfg.mqUser();
        String mqPassword = cfg.mqPassword();

        boolean useMq = pt.getBoolean("use.mq", true);

        // --- MQ stream (или тестовый JSON) ---
        final DataStream<MqRecord> mqStream;

        if (useMq) {
            mqStream = env.addSource(
                            new MqSource(
                                    cfg.mqHost(),
                                    cfg.mqPort(),
                                    cfg.mqChannel(),
                                    cfg.mqQmgr(),
                                    cfg.mqInQueue(),
                                    mqUser,
                                    mqPassword
                            ),
                            "mq-source"
                    )
                    .name("mq-source")
                    .setParallelism(1);
        } else {
            log.warn("TEST MODE: using hardcoded JSON instead of MQ. Set --use.mq=true to read from MQ.");
            mqStream = env
                    .fromElements(new MqRecord(new byte[0], TEST_JSON))
                    .name("test-json-source")
                    .setParallelism(1);
        }

// --- Kafka control stream ---
        String bootstrap = pt.get("kafka.bootstrap", "localhost:9092");
        String topic = pt.get("kafka.topic", "dfw-rules");
        String group = pt.get("kafka.group", "dfw-rules-group");

        KafkaSource<CacheUpdateEvent> kafkaSource = KafkaSource.<CacheUpdateEvent>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(topic)
                .setGroupId(group)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new CacheUpdateEventDeserializationSchema())
                .build();

        DataStream<CacheUpdateEvent> updates = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "rules-kafka")
                .name("rules-kafka")
                .setParallelism(1);

// broadcast state: храним последнее событие обновления кэша
        MapStateDescriptor<String, CacheUpdateEvent> bcDesc =
                new MapStateDescriptor<>(
                        "cache-update-broadcast",
                        Types.STRING,
                        Types.POJO(CacheUpdateEvent.class)
                );

        BroadcastStream<CacheUpdateEvent> bcUpdates = updates.broadcast(bcDesc);

// --- connect MQ + broadcast updates ---
        DataStream<MqReply> processed = mqStream
                .connect(bcUpdates)
                .process(new MqWithRulesReloadBroadcastProcessFunction(bcDesc))
                .name("mq-process-with-rules-reload")
                .setParallelism(1);

        processed
                .filter(Objects::nonNull)
                .name("filter-non-null")
                .setParallelism(1)
                .addSink(new MqSink(
                        cfg.mqHost(),
                        cfg.mqPort(),
                        cfg.mqChannel(),
                        cfg.mqQmgr(),
                        cfg.mqOutQueue(),
                        mqUser,
                        mqPassword
                ))
                .name("mq-sink")
                .setParallelism(1);

        env.execute("DataFirewall IBM MQ Job (SHORT ANSWER) + Kafka Rules Reload");
    }

    private static String[] normalizeArgs(String[] args) {
        if (args == null || args.length == 0) return new String[0];
        List<String> out = new ArrayList<>();
        for (String a : args) {
            if (a == null) continue;
            if (a.startsWith("--") && a.contains("=")) {
                int idx = a.indexOf('=');
                out.add(a.substring(0, idx));
                out.add(a.substring(idx + 1));
            } else out.add(a);
        }
        return out.toArray(new String[0]);
    }
}