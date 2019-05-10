-- version 1.0.0 update 2019/3/19

create table if not exists hb_hospitals
(
  hospital_id   varchar(20) primary key,
  hospital_name varchar(50) not null,
  contacts      varchar(20) not null,
  phone         varchar(11) not null,
  region        varchar(30),
  address       varchar(60),
  active        varchar(1)  not null default '0', -- 逻辑删除状态 0：存在未被删除，1：已被删除
  time          integer
);

create table if not exists hb_equipments
(
  equipment_id varchar(100) primary key,
  hospital_id  varchar(20) not null references hb_hospitals (hospital_id) on update cascade on delete restrict,
  time         integer
);

create table if not exists hb_doctors
(
  id           SERIAL,
  doctor_phone varchar(11) primary key,
  hospital_id  varchar(20) not null references hb_hospitals (hospital_id) on update cascade on delete restrict,
  active       varchar(1)  not null default '0', -- 逻辑删除状态 0：存在未被删除，1：已被删除
  time         integer
);

create table if not exists hb_patients
(
  id                SERIAL,
  patient_phone     varchar(11) primary key,
  doctor_phone      varchar(11) not null,
  patient_name      varchar(20) not null,
  gender            varchar(1)  not null, -- 性别 0：男，1：女
  birthday          date,
  region            varchar(30),
  address           varchar(60),
  medical_history   text,
  before_healthcare varchar(100),
  after_healthcare  varchar(100),
  remark            varchar(60),
  time              integer
);

create table if not exists hb_recharge_logs
(
  id              varchar(20) primary key,
  equipment_id    varchar(100),
  recharge_counts integer,
  user_phone      varchar(11),
  state           varchar(1) not null default '0', -- 充值状态 0：正在充值中，1：充值成功，2：充值失败
  time            integer,
  recharge_time   integer,                         -- 充值成功时间
  store_aval      bigint                           -- 充值前可用次数
);
create index hb_recharge_logs_equipment_id_index on hb_recharge_logs (equipment_id);

create table if not exists hb_treatment_logs
(
  id            SERIAL primary key,
  patient_id    varchar(20)  not null,
  equipment_id  varchar(100) not null,
  doctor_id     varchar(20)  not null,
  hospital_id   varchar(20)  not null,
  body_parts    varchar(20)[] default '{}',
  gear_position smallint[]    default '{}',
  pulse_counts  integer[]     default '{}',
  remark        varchar(60),
  time          integer
);
create index hb_treatment_logs_patient_id_index on hb_treatment_logs (patient_id);

create table if not exists hb_devices
(
  id         varchar(100) primary key, -- 设备id
  mac_no     varchar(50),              -- 采集器id
  type       varchar(20) not null,     -- 设备类型
  name       varchar(50) not null,     -- 设备名称
  aval_times bigint,                   -- 可用次数
  totl_val   bigint                    -- 总次数
);

create table if not exists hb_roles
(
  phone   varchar(11) primary key,
  rights  varchar(20) not null,
  time    integer,
  factory varchar(20) default 'hbyl'
);

-- version 1.0.0 update 2019/4/11
alter table hb_roles add invitor varchar(11);  -- 添加普通管理员时的邀请人