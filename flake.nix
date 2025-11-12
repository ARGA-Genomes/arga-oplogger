{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    naersk = {
      url = "github:nix-community/naersk";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      naersk,
    }@inputs:
    let
      system = "x86_64-linux";
      pkgs = import nixpkgs { inherit system; };
      naersk' = pkgs.callPackage naersk { };
    in
    rec {
      packages.${system} = rec {
        # build the oplogger executable
        oplogger = naersk'.buildPackage {
          name = "arga-oplogger";
          pname = "arga-oplogger";
          src = ./.;
          nativeBuildInputs = [ pkgs.postgresql.lib ];
        };

        # build the container image
        oci = pkgs.dockerTools.buildLayeredImage {
          name = "oplogger";
          tag = "latest";

          contents = [
            oplogger
          ];

          config = {
            WorkingDir = "/";
            Env = [
              "DATABASE_URL=postgres://arga@localhost/arga"
            ];
            ExposedPorts = { };
            Cmd = [ "/bin/oplogger" ];
            Labels = {
              "org.opencontainers.image.source" = "https://github.com/ARGA-Genomes/arga-oplogger";
              "org.opencontainers.image.url" = "https://github.com/ARGA-Genomes/arga-oplogger";
              "org.opencontainers.image.description" = "A container image with the oplogger tool for imports";
              "org.opencontainers.image.licenses" = "AGPL-3.0-or-later";
              "org.opencontainers.image.authors" = "ARGA Team <support@arga.org.au>";
            };
            Volumes = {
              "/data" = { };
            };
          };
        };

        default = oplogger;
      };
    };
}
