<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" version="2.0">
  <!--Tests with absolute paths
  <xsl:import href="file:/C:/Users/geoffroy/Documents/GitHub/XSLT_signalement_docelec/xslt/tables_conversion.xsl"/>-->
  <!--<xsl:import href="string-join((string-join(tokenize(static-base-uri(),'/')[position()!=last()],'/'),'tables_conversion.xsl'),'/')"/>
  -->
  <xsl:import href="tables_conversion.xsl"/>
  <xsl:output method="xml" version="1.0" encoding="UTF-8" indent="yes"/>
   <xsl:strip-space elements="*"/>
   <!--<xsl:param name="xml_file_name" select="concat($root,'/atoz/result_files/atoz.xml')" />-->
   <!--variables de tables_conversion.xsl-->
     <xsl:variable name="all_specific_titles">
     <!--on encadre les valeurs dans des | pour pouvoir comparer les noms exacts des packages - et titres - dans le cas de répétitions d'occurrence telless que sciendirect, sciencedirect (licences nationales) etc...-->
      <xsl:variable name="temp">
         <xsl:for-each select="$specific/entry">
            <xsl:value-of select="concat('|',normalize-space(Title),'|')"/>
         </xsl:for-each>
      </xsl:variable>
      <xsl:value-of select="normalize-space($temp)"/>
   </xsl:variable>
   <xsl:variable name="all_standard_packages">
      <xsl:variable name="temp">
         <xsl:for-each select="$standard/entry">
            <xsl:value-of select="concat('|',normalize-space(PackageName),'|')"/>
         </xsl:for-each>
      </xsl:variable>
      <xsl:value-of select="normalize-space($temp)"/>
   </xsl:variable>
   
   <!--result-document template
   <xsl:template match="/">
  <xsl:result-document href="{$xml_file_name}">
    <xsl:apply-templates/>
  </xsl:result-document>
</xsl:template>-->

   <!--identity template-->
   <xsl:template match="node()|@*">
     <xsl:copy>
         <xsl:apply-templates select="node()|@*"/>
     </xsl:copy>
   </xsl:template>
  
   <xsl:template match="Resource">
      <xsl:variable name="source" select="normalize-space(PackageName)"/>
      <xsl:variable name="title" select="normalize-space(Title)"/>
      <xsl:variable name="rtype" select="normalize-space(ResourceType)"/>
      <xsl:if test="$rtype != 'Report'">
         <xsl:copy>
            <xsl:apply-templates select="node()|@*"/>
            <!--increment counter with position : unused, become unecessary with the python process wich exports the dataframe index as increment id
         <LinkId>
            <xsl:value-of select="position()"/> 
         </LinkId>
         -->
            <LinkId>
               <xsl:value-of select="index + 1"/>
            </LinkId>
            <ResourceId>
                <!--<xsl:value-of select="KBID"/>-->
                <xsl:value-of select="concat(KBID,substring(translate($source, 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'),1,4))"/>
            </ResourceId>
            <Source>
              <xsl:value-of select="$source"/>
            </Source>
            <!--new xml entry-->
            <ManagedCoverage>
               <xsl:variable name="start">
                  <xsl:call-template name="coverage">
                     <xsl:with-param name="date" select="ManagedCoverageBegin"/>
                  </xsl:call-template>
               </xsl:variable>
               <xsl:variable name="end">
                  <xsl:call-template name="coverage">
                     <xsl:with-param name="date" select="ManagedCoverageEnd"/>
                  </xsl:call-template>
               </xsl:variable>
               <xsl:choose>
                  <xsl:when test="$rtype = 'Book'">
                     <xsl:value-of select="$start"/>
                  </xsl:when>
                  <xsl:otherwise>
                     <xsl:choose>
                        <xsl:when test="$end = 'present'">
                           <xsl:value-of select="concat($start,' to ',$end)"/> 
                        </xsl:when>
                        <xsl:otherwise>
                           <xsl:value-of select="concat($start,' - ',$end)"/>
                        </xsl:otherwise>
                     </xsl:choose>
                  </xsl:otherwise>
               </xsl:choose>
            </ManagedCoverage>
            <!--new entries by calling external template-->
            <xsl:call-template name="notes_and_accesstype">
                  <xsl:with-param name="title" select="$title"/>
                   <xsl:with-param name="source" select="$source"/>
            </xsl:call-template>
         </xsl:copy>
      </xsl:if>
   </xsl:template>

     <!-- remove elements -->
  <xsl:template match="ManagedCoverageBegin"/>
  <xsl:template match="ManagedCoverageEnd"/>
  <xsl:template match="index"/>
  <xsl:template match="KBID"/>
  <xsl:template match="PackageName"/>
  
   <!--common template to manage date format-->
  <xsl:template name="coverage">
      <xsl:param name="date"/>
      <xsl:choose>
         <xsl:when test="$date = 'Present'">
            <xsl:value-of select="'present'"/>
         </xsl:when>
         <xsl:when test="string-length($date) = 10 and contains($date, '-')">
            <xsl:value-of select="substring($date,1,4)"/>
         </xsl:when>
         <xsl:otherwise>
            <xsl:value-of select="$date"/>
         </xsl:otherwise>
      </xsl:choose>
      </xsl:template>

   <!--template adding notes & accesstype by comparing with tables_conversion-->
       <xsl:template name="notes_and_accesstype">
      <xsl:param name="title"/>
      <xsl:param name="source"/>
      <xsl:choose>
         <xsl:when test="contains($all_specific_titles,concat('|',$title,'|'))">
            <AccessType>
               <xsl:value-of select="$specific/entry[Title = $title]/AccessType"/>
            </AccessType>
            <Note>
               <xsl:value-of select="$specific/entry[Title = $title]/Note"/>
            </Note>
         </xsl:when>
         <xsl:when test="not(contains($all_specific_titles,concat('|',$title,'|'))) and contains($all_standard_packages,concat('|',$source,'|'))">
            <AccessType>
               <xsl:value-of select="$standard/entry[PackageName = $source]/AccessType"/>
            </AccessType>
            <Note>
               <xsl:value-of select="$standard/entry[PackageName = $source]/Note"/>
            </Note>
         </xsl:when>
        <xsl:when test="not(contains($all_specific_titles,concat('|',$title,'|'))) and not(contains($all_standard_packages,concat('|',$source,'|')))">
            <AccessType>
               <xsl:value-of select="'0'"/>
            </AccessType>
            <Note/>
         </xsl:when>
      </xsl:choose>
      </xsl:template>

</xsl:stylesheet>
