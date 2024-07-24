{ pkgs, inputs, ... }:

{
  packages = with pkgs; [
    postgresql
  ];

  languages.rust = {
    enable = true;
    components = [ "rustc" "cargo" "clippy" "rustfmt" "rust-analyzer" ];
    toolchain = {
      rustfmt = inputs.fenix.packages.${pkgs.system}.latest.rustfmt;
    };
  };

  pre-commit.hooks = {
    clippy.enable = true;
  };

  cachix.enable = false;
}
