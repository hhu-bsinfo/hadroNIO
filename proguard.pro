# For some reason, ProGuard will not work with warnings enabled, although no warnings are generated
-ignorewarnings

# Obfuscation does not make sense for a library
-dontobfuscate

# These classes form the public API of hadroNIO, so they need to be marked as entry points
-keep,allowoptimization class de.hhu.bsinfo.hadronio.Hadronio* { *; }

# For some reason, ProGuard obfuscates these callbacks
-keep class de.hhu.bsinfo.hadronio.SendCallback*$ReadHandler { *; }
-keep class de.hhu.bsinfo.hadronio.HadronioSocketChannel*$ReadHandler { *; }

# The interfaces need to be kept, since they are implemented by the bindings
-keep interface de.hhu.bsinfo.hadronio.binding.* { *; }

# Keep the JUCX binding
-keep class de.hhu.bsinfo.hadronio.jucx.* { *; }

# Keep the infinileap binding
-keep class de.hhu.bsinfo.hadronio.infinileap.* { *; }