<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="2.0">
  <xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes"/>
   <!--<xsl:strip-space elements="*"/>-->
    <xsl:template match="/*">
      <data>
         <xsl:for-each select="row">
            <row>
               <xsl:variable name="ppn">
                  <xsl:value-of select="concat('PPN',PPN)"/>
               </xsl:variable>
               <ppn>
                  <xsl:value-of select="PPN"/>
               </ppn>
               <xsl:call-template name="aleph_find_ppn">
                  <xsl:with-param name="ppn" select="$ppn"/>
               </xsl:call-template>
            </row>
         </xsl:for-each>
      </data>
   </xsl:template> 

   <xsl:template name="aleph_find_ppn">
      <xsl:param name="ppn"/>
      <xsl:variable name="set_number">
         <xsl:value-of select="document(concat('http://primo-prod.unice.fr:8080/X?op=find&amp;base=UNS01&amp;request=',$ppn,'&amp;code=idn'))//set_number"/>
      </xsl:variable>
      <xsl:variable name="no_entries">
         <xsl:value-of select="document(concat('http://primo-prod.unice.fr:8080/X?op=find&amp;base=UNS01&amp;request=',$ppn,'&amp;code=idn'))//no_entries"/>
      </xsl:variable>
      <xsl:variable name="doc_number">
        <xsl:value-of select="document(concat('http://primo-prod.unice.fr:8080/X?op=present&amp;base=UNS01&amp;set_number=',$set_number,'&amp;set_entry=',$no_entries))//record/doc_number"/>
      </xsl:variable>
      <set_number>
         <xsl:value-of select="$set_number"/>
      </set_number>
      <no_entries>
         <xsl:value-of select="$no_entries"/>
      </no_entries>
      <doc_number>
         <xsl:value-of select="concat('ALP',$doc_number)"/>
      </doc_number>
      </xsl:template>
  
</xsl:stylesheet>


