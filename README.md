# Mini DataBase Management System (NITCBase)

## Overview

NITCBase is a mini DataBase Management System developed as part of a university laboratory course. It is a lightweight database system designed for educational purposes, allowing users to perform basic database operations. This README file provides essential information for setting up and running NITCBase.

## Table of Contents

-   [Prerequisites](#prerequisites)
-   [Getting Started](#getting-started)
    -   [Installation](#installation)
    -   [Running NITCBase](#running-nitcbase)
-   [Usage](#usage)
-   [XFS-Documentation](#xfs-documentation)
-   [Contributing](#contributing)
-   [License](#license)

## Prerequisites

Before running NITCBase, ensure that you have the following prerequisites installed on your system:

-   Linux-based operating system (tested on Ubuntu)
-   C/C++ compiler (gcc/g++)
-   Git

## Getting Started

Follow these steps to set up and run NITCBase on your system:

### Installation

1. Clone the NITCBase repository to your local machine:

    ```bash
    git clone https://github.com/tbharath03/NITCBase.git
    ```

2. Navigate to the project directory:

    ```bash
    cd NITCBase/mynitcbase
    ```

3. Build the NITCBase application using the `make` command:

    ```bash
    make
    ```

4. In case you run into any issues while creating builds, install the necessary packages.

### Running NITCBase

Once you have successfully built NITCBase, you can run it with the following command:

```bash
./nitcbase
```

## Usage

NITCBase provides a simple command-line interface for executing basic database operations. You can interact with the database using various SQL commands. For detailed information on available commands and their usage, refer to the XFS-Documentation provided at [link](https://nitcbase.github.io/docs/User%20Interface%20Commands/).

## NITCBase Documentation

For detailed information on architecture, methodology, design and roadmap, please refer to the [NITCBase Documentation](https://nitcbase.github.io/).

## Contributing

We welcome contributions to NITCBase! If you would like to contribute to this project, please create a pull request and make sure to follow decent naming conventions. Upon review, I shall merge your request if found useful.

## License

## NITCBase is licensed under the MIT License.

Thank you for using NITCBase. If you encounter any issues or have questions, please don't hesitate to [create an issue](https://github.com/your-username/NITCBase/issues) on our GitHub repository.
