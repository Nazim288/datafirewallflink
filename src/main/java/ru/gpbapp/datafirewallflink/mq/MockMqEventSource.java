package ru.gpbapp.datafirewallflink.mq;

import java.util.List;
import java.util.stream.Stream;

/**
 * Тестовый MQ с заранее заданными сообщениями.
 */
public final class MockMqEventSource implements EventSource {

    private static final List<String> TEST_MESSAGES = List.of(
            """
            {
              "dw_created_dttm": "2025-11-25T10:24:04.442Z",
              "dw_user_login": "test_user",
              "dw_action_dttm": "2025-11-25T10:24:14.239Z",
              "dfw_hostname": "test-host",
              "dfw_query_id": "TEST_QUERY_001",
              "dfw_action_type": "QUERY",
              "dfw_dataset_code": "TEST_DASHBOARD",
              "data": {
                "baseInfo": {
                  "fullName": "Ivan Иванов",
                  "name": "Ivan",
                  "surname": "Иванов",
                  "patronymic": "Сергеевич",
                  "gender": "M",
                  "citizenship": "RU",
                  "birthdate": "1995-06-12",
                  "birthPlace": "Москва",
                  "birthCountry": "РОССИЯ",
                  "isPatronymicLack": false,
                  "dataset_code": "TEST_CLIENT_DATA",
    
                  "mapping.fullName": "ОСНОВНЫЕ СВЕДЕНИЯ,ФИО одной строкой",
                  "mapping.name": "ОСНОВНЫЕ СВЕДЕНИЯ.Имя",
                  "mapping.surname": "ОСНОВНЫЕ СВЕДЕНИЯ,Фамилия",
                  "mapping.patronymic": "ОСНОВНЫЕ СВЕДЕНИЯ.Отчество",
                  "mapping.gender": "ОСНОВНЫЕ СВЕДЕНИЯ.Пол",
                  "mapping.citizenship": "ОСНОВНЫЕ СВЕДЕНИЯ.Гражданство",
                  "mapping.birthdate": "ОСНОВНЫЕ СВЕДЕНИЯ-Дата рождения",
                  "mapping.birthPlace": "ОСНОВНЫЕ СВЕДЕНИЯ Место рождения",
                  "mapping.birthCountry": "none",
                  "mapping.isPatronymicLack": "ОСНОВНЫЕ СВЕДЕНИЯ.Признак отсутствия отчества"
                },
                "documents": {
                  "clientInn": "7707083893",
                  "clientSnils": "112-233-445 95",
                  "dataset_code": "TEST_CLIENT_DOCS",
    
                  "mapping.clientSnils": "ОСНОВНЫЕ СВЕДЕНИЯ.СНИЛС",
                  "mapping.clientInn": "ИНН.Номер свидетельства",
    
                  "clientIdCard": [
                    {
                      "elemId": "PASSPORT_RU",
                      "type": "PASSPORT_RU",
                      "series": "4510",
                      "number": "123456",
                      "departmentCode": "770-001",
                      "issueDate": "2018-04-12",
                      "expiryDate": "2040-10-28",
                      "issueAuthority": "ОВД Тверского района г. Москвы",
                      "primary": true,
    
                      "mapping.type": "ДУЛ.Паспорт РФ.Тип ДУЛ",
                      "mapping.series": "ДУЛ.Паспорт РФ.Серия",
                      "mapping.number": "ДУЛ.Паспорт РФ.Номер",
                      "mapping.departmentCode": "ДУЛ.Паспорт РФ.Код подразделения",
                      "mapping.issueDate": "ДУЛ.Паспорт РФ.Дата выдачи",
                      "mapping.expiryDate": "none",
                      "mapping.issueAuthority": "ДУЛ.Паспорт РФ.Кем выдан",
                      "mapping.primary": "ДУЛ.Паспорт РФ.Признак основного"
                    }
                  ]
                }
              },
              "dfw_readed_from_mq_dttm": "2025-11-25T10:24:04.166Z"
            }
            """
    );

    @Override
    public Stream<String> stream() {
        return TEST_MESSAGES.stream();
    }

    @Override
    public void close() {}
}
