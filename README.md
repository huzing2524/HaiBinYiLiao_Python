海滨医疗项目后端API接口服务

<table>
   <tr>
      <td>API</td>
      <td>任务 resource </td>
      <td>操作 type </td>
      <td>资源 params </td>
      <td>说明</td>
      <td>发送消息</td>
   </tr>
   <tr>
      <td rowspan="1">hb/recharge/new</td>
      <td>PyEquipmentRecharge</td>
      <td>POST</td>
      <td>
          {
              "uuid": "string",
              "phone": "string,
              "equipment_id": "string",
              "recharge_counts": "integer",
          }
      </td>
      <td>设备使用次数充值</td>
      <td></td>
   </tr>
</table>