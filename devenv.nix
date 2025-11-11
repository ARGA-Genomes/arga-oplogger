{ pkgs, inputs, ... }:

{
  packages = with pkgs; [
    postgresql.lib
    rocksdb
    llvmPackages_21.libcxxClang
  ];

  languages.rust = {
    enable = true;
    components = [
      "rustc"
      "cargo"
      "clippy"
      "rustfmt"
      "rust-analyzer"
    ];
    toolchain = {
      rustfmt = inputs.fenix.packages.${pkgs.system}.latest.rustfmt;
    };
  };

  env.LIBCLANG_PATH = "${pkgs.llvmPackages_21.libclang.lib}/lib";

#  git-hooks.hooks = {
#    clippy.enable = true;
#  };

  cachix.enable = false;
  dotenv.disableHint = true;
}
