#!/usr/bin/perl -0777 -pi

# -0777 will read entire file if no input separator is specified

$origXML = '<!--\n  ~ Copyright 2014 Commonwealth Computer Research, Inc.\n  ~\n  ~ Licensed under the Apache License, Version 2.0 \(the \"License\"\);\n  ~ you may not use this file except in compliance with the License.\n  ~ You may obtain a copy of the License at\n  ~\n  ~ http://www.apache.org/licenses/LICENSE-2.0\n  ~\n  ~ Unless required by applicable law or agreed to in writing, software\n  ~ distributed under the License is distributed on an \"AS IS\" BASIS,\n  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n  ~ See the License for the specific language governing permissions and\n  ~ limitations under the License.\n  -->';

$origCode = '^/.* \* Copyright 2014 Commonwealth Computer Research, Inc.\n \*\n \* Licensed under the Apache License, Version 2.0 \(the \"License\"\);\n \* you may not use this file except in compliance with the License.\n \* You may obtain a copy of the License at\n \*\n \* http://www.apache.org/licenses/LICENSE-2.0\n \*\n \* Unless required by applicable law or agreed to in writing, software\n \* distributed under the License is distributed on an \"AS IS\" BASIS,\n \* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n \* See the License for the specific language governing permissions and\n \* limitations under the License.\n \*/';


$replaceXML = '<!--********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This free program and the accompanying materials
* are made available only under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php. This software is distributed on an
* "as-is" basis, without warranties or conditions of any kind, either express or implied.
*********************************************************************-->';

$replaceCode = '/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This free program and the accompanying materials
* are made available only under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php. This software is distributed on an
* "as-is" basis, without warranties or conditions of any kind, either express or implied.
*************************************************************************/';
#print "Replace command\n";
#print "s|$origCode|$replaceCode|s;\n";
#print "end\n";
s|$origCode|$replaceCode|s;
# s|$origXML|$replaceXML|m;
