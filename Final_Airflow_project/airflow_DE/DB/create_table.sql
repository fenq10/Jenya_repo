CREATE TABLE if not exists ga_sessions
(
session_id char(100) PRIMARY KEY
,client_id char(30)
,visit_number int         
,utm_source char(30)        
,utm_medium char(30)        
,utm_campaign char(30)        
,utm_adcontent char(30)        
,device_category char(30)
,device_os char(30)      
,device_brand char(30)        
,device_screen_resolution char(15)        
,device_browser char(50)        
,geo_country char(40)        
,geo_city char(40)        
,visit_datetime timestamp
);

CREATE TABLE if not exists ga_hits
(
session_id char(100),
hit_number int,
hit_type char(30)    ,      
hit_referer char(100)  ,      
hit_page_path varchar(2000)  ,      
event_category char(30)   ,     
event_action char(50)   ,     
event_label char(30),
hit_datetime timestamp
--,FOREIGN KEY (session_id) REFERENCES ga_sessions(session_id )   --внешний ключ убрал для тестирования  
)

