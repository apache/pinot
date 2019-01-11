<#macro addBlock title align>
    <tr>
      <td style="border-bottom: 1px solid rgba(0,0,0,0.15); padding: 12px 24px; align:${align}">
        <#if title?has_content>
          <p style="font-size:20px; line-height:24px; color:#1D1D1D; font-weight: 500; margin:0; padding:0;">${title}</p>
        </#if>

        <#nested>

      </td>
    </tr>
</#macro>