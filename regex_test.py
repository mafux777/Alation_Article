# coding=utf8
# the above tag defines encoding for this document and is for Python 2.x compatibility

import re

regex = r"<a +data-oid=\"(\d+)\" +data-otype=\"(article)\" +href=\"/article/(\d+)/[^>]*\">"

test_str = ("<div>\n\n"
            "	<p>Successful enterprise implementation of a data catalog is complex. This guide explains key components, roles, processes and techniques that should be considered.</p>\n\n"
            "	<h2>Table of Contents</h2>\n\n"
            "	<h3><a data-oid=\"53\" data-otype=\"article\" href=\"/article/53/a-introduction\">A. Introduction &amp; Use</a>&nbsp;</h3>\n\n"
            "	<p><a data-oid=\"190\" data-otype=\"article\" href=\"/article/190/\">B. Document History</a>&nbsp;</p>\n\n"
            "	<h3><strong>Section I - Fundamentals</strong></h3>\n\n"
            "	<p><a data-oid=\"55\" data-otype=\"article\" href=\"/article/55/\">Chapter 01: Enterprise Goals and Objectives</a>&nbsp;</p>\n\n"
            "	<p><a data-oid=\"56\" data-otype=\"article\" href=\"/article/56/\">Chapter 02: Declaring an Enterprise Program for Cataloging</a></p>\n\n"
            "	<p><a data-oid=\"57\" data-otype=\"article\" href=\"/article/57/chapter-3-scoping-an-enterprise-implementation\">Chapter 03: Scoping an Enterprise Implementation</a>&nbsp;</p>\n\n"
            "	<p><a data-oid=\"58\" data-otype=\"article\" href=\"/article/58/\">Chapter 04: Data Catalog Maturity Model</a></p>\n\n"
            "	<p><a data-oid=\"46\" data-otype=\"article\" href=\"/article/46/chapter-05-the-alation-customer-journey-a-lifecycle-model\">Chapter 05: The Customer Journey - A Lifecycle Model</a></p>\n\n"
            "	<p><a data-oid=\"60\" data-otype=\"article\" href=\"/article/60/chapter-6-enterprise-breadth-and-depth\">Chapter 06: Enterprise Breadth and Depth</a>&nbsp;</p>\n\n"
            "	<p><a data-oid=\"61\" data-otype=\"article\" href=\"/article/61/\">Chapter 07: Resource Model, Roles &amp; Responsibilities</a>&nbsp;</p>\n\n"
            "	<p><a data-oid=\"62\" data-otype=\"article\" href=\"/article/62/chapter-8-data-catalog-program-office\">Chapter 08: Data Catalog Program Office</a>&nbsp;</p>\n\n"
            "	<p><a data-oid=\"63\" data-otype=\"article\" href=\"/article/63/\">Chapter 09: Executive Sponsorship and Key Stakeholders</a>&nbsp;</p>\n\n"
            "	<p><a data-oid=\"65\" data-otype=\"article\" href=\"/article/65/\">Chapter 10: Governance, Data Management and Cataloging</a>&nbsp;</p>\n\n"
            "	<p><a data-oid=\"66\" data-otype=\"article\" href=\"/article/66/\">Chapter 11: Franchise and Shared Services Model</a>&nbsp;</p>\n\n"
            "	<p><a data-oid=\"67\" data-otype=\"article\" href=\"/article/67/\">Chapter 12: Approach to Rollout Prioritization</a>&nbsp;</p>\n\n"
            "	<p><a data-oid=\"68\" data-otype=\"article\" href=\"/article/68/\">Chapter 13: Application Types</a>&nbsp;</p>\n\n"
            "	<p><a data-oid=\"69\" data-otype=\"article\" href=\"/article/69/\">Chapter 14: Data Models (Enterprise, Conceptual &amp; Logical)</a>&nbsp;</p>\n\n"
            "	<p><a data-oid=\"70\" data-otype=\"article\" href=\"/article/70/\">Chapter 15: Catalog Design</a>&nbsp;</p>\n\n"
            "	<p><a data-oid=\"76\" data-otype=\"article\" href=\"/article/76/\">Chapter 16: Communication Planning</a>&nbsp;</p>\n\n"
            "	<p><a data-oid=\"77\" data-otype=\"article\" href=\"/article/77/\">Chapter 17: Branding</a>&nbsp;</p>\n\n"
            "	<p><a data-oid=\"78\" data-otype=\"article\" href=\"/article/78/\">Chapter 18: Adoption Techniques</a></p>\n\n"
            "	<p><a data-oid=\"103\" data-otype=\"article\" href=\"/article/103/\">Chapter 19: Tier 1 Application Support and Operations</a>&nbsp;</p>\n\n"
            "	<p><a data-oid=\"104\" data-otype=\"article\" href=\"/article/104/\">Chapter 20: Usage Policies &amp; Guidelines</a>&nbsp;</p>\n\n"
            "	<p><a data-oid=\"127\" data-otype=\"article\" href=\"/article/127/chapter-21-catalog-architecture-and-sizing\">Chapter 21: Platform Topology and Sizing</a>&nbsp;</p>\n\n"
            "	<p><a data-oid=\"128\" data-otype=\"article\" href=\"/article/128/chapter-22-aligned-and-guided-stewardship\">Chapter 22: Agile and Guided Stewardship</a>&nbsp;</p>\n\n"
            "	<p><a data-oid=\"129\" data-otype=\"article\" href=\"/article/129/\">Chapter 23: Usage Measurement and Data Value Approach</a>&nbsp;</p>\n\n"
            "	<p><a data-oid=\"150\" data-otype=\"article\" href=\"/article/150/\">Chapter 24: Catalog Visibility and Security Model</a>&nbsp;</p>\n\n"
            "	<p><a data-oid=\"161\" data-otype=\"article\" href=\"/article/161/\">Chapter 25: Building a Business Case and ROI</a>&nbsp;</p>\n\n"
            "	<p><a data-oid=\"185\" data-otype=\"article\" href=\"/article/185/\">Chapter 26: Data Quality and the Data Catalog</a>&nbsp;</p>\n\n"
            "	<h3>Section II - Implementation</h3>\n\n"
            "	<p>\n"
            "		<a href=\"https://docs.google.com/document/d/1eTM_ux_DgmOGgUFM7k5pAA1bqtjgBzEyleS6vuAN7Bc/edit#heading=h.3xn3t6xz1kmb\"></a>\n"
            "	</p>\n\n"
            "	<p><a data-oid=\"30\" data-otype=\"article\" href=\"/article/30/chapter-26-right-start-service\">Chapter 27: Right Start Service</a></p>\n\n"
            "	<p><a data-oid=\"175\" data-otype=\"article\" href=\"/article/175/chapter-27-alation-analytics\">Chapter 28: Alation Analytics</a>&nbsp;</p>\n\n"
            "	<p><a data-oid=\"176\" data-otype=\"article\" href=\"/article/176/chapter-28-advanced-profiling-query-templates\">Chapter 29: Advanced Profiling Query Templates</a>&nbsp;</p>\n\n"
            "	<p><a data-oid=\"158\" data-otype=\"article\" href=\"/article/158/\">Chapter 30: Getting Started Content Templates</a>&nbsp;</p>\n"
            "</div>\n")

matches = re.finditer(regex, test_str, re.MULTILINE)

for matchNum, match in enumerate(matches, start=1):

    print("Match {matchNum} was found at {start}-{end}: {match}".format(matchNum=matchNum, start=match.start(),
                                                                        end=match.end(), match=match.group()))

    for groupNum in range(0, len(match.groups())):
        groupNum = groupNum + 1

        print("Group {groupNum} found at {start}-{end}: {group}".format(groupNum=groupNum, start=match.start(groupNum),
                                                                        end=match.end(groupNum),
                                                                        group=match.group(groupNum)))

# Note: for Python 2.7 compatibility, use ur"" to prefix the regex and u"" to prefix the test string and substitution.
